

from __future__ import annotations

import os
import sys
import logging
from dotenv import load_dotenv

# ── Load env ──────────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
)
logger = logging.getLogger("dataguard.etl")

# ── Java 17 fix required by Spline 2.x ────────────────────────────────────────
_OPENS = "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
if _OPENS not in os.environ.get("JDK_JAVA_OPTIONS", ""):
    os.environ["JDK_JAVA_OPTIONS"] = (
        os.environ.get("JDK_JAVA_OPTIONS", "") + " " + _OPENS
    ).strip()

# ── Connection config ──────────────────────────────────────────────────────────
MARIADB_HOST     = os.getenv("MARIADB_HOST",     "127.0.0.1")
MARIADB_PORT     = os.getenv("MARIADB_PORT",     "3307")
MARIADB_USER     = os.getenv("MARIADB_USER",     "root")
MARIADB_PASS     = os.getenv("MARIADB_PASS",     "root123")
MARIADB_DATABASE = os.getenv("MARIADB_DATABASE", "governance_db")

POSTGRES_URL         = os.getenv("POSTGRES_URL", "postgresql://myuser:mypassword@localhost:5432/dataguard")
TARGET_DB_NAME       = os.getenv("TARGET_DB_NAME", "company_data")
SPLINE_PRODUCER_URL  = os.getenv("SPLINE_PRODUCER_URL", "http://localhost:8080/producer")

SPARK_MASTER             = os.getenv("SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")

SPLINE_AGENT_VERSION   = os.getenv("SPLINE_AGENT_VERSION",   "2.2.1")
MARIADB_DRIVER_VERSION = os.getenv("MARIADB_DRIVER_VERSION", "3.3.3")
POSTGRES_DRIVER_VERSION= os.getenv("POSTGRES_DRIVER_VERSION","42.7.3")

DB_NULL_SENTINEL = os.getenv("DB_NULL_SENTINEL", "(NULL)")
EMAIL_BROKEN_CHAR = os.getenv("EMAIL_BROKEN_CHAR", "#")

MARIADB_DRIVER  = "org.mariadb.jdbc.Driver"
POSTGRES_DRIVER = "org.postgresql.Driver"

SOURCE_JDBC = f"jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DATABASE}"

# Parse postgres URL for JDBC target
def _pg_jdbc() -> tuple[str, str, str]:
    """Return (jdbc_url, user, password) for the target PostgreSQL DB."""
    rest = POSTGRES_URL.split("://", 1)[1].split("?")[0]
    creds, conn = rest.split("@", 1)
    host_port, _ = conn.split("/", 1)
    user, password = (creds.split(":", 1) + [""])[:2]
    return f"jdbc:postgresql://{host_port}/{TARGET_DB_NAME}", user, password


# ── Tables to ETL ─────────────────────────────────────────────────────────────
# Add or remove table names here to control which tables are processed.
TABLES = ["customers", "orders", "products"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Spark session with Spline Agent
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def create_spark_session(app_name: str):
    from pyspark.sql import SparkSession

    packages = ",".join([
        f"za.co.absa.spline.agent.spark:spark-3.5-spline-agent-bundle_2.12:{SPLINE_AGENT_VERSION}",
        f"org.mariadb.jdbc:mariadb-java-client:{MARIADB_DRIVER_VERSION}",
        f"org.postgresql:postgresql:{POSTGRES_DRIVER_VERSION}",
    ])

    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.jars.packages",              packages)
        .config("spark.spline.producer.url",         SPLINE_PRODUCER_URL)
        .config("spark.spline.mode",                 "ENABLED")
        .config("spark.ui.enabled",                  "false")
        .config("spark.sql.shuffle.partitions",      SPARK_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )

    # Activate Spline Agent via JVM bridge (required for Spline 2.x)
    init_class = getattr(spark._jvm, "za.co.absa.spline.harvester.SparkLineageInitializer$")
    getattr(init_class, "MODULE$").enableLineageTracking(spark._jsparkSession)
    logger.info("Spline Agent active → %s", SPLINE_PRODUCER_URL)
    return spark


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Transformations  (each withColumn is tracked by Spline for column lineage)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def transform(df, primary_key: str | None = None):
    from pyspark.sql import functions as F

    # 1. Deduplicate on primary key
    pk = primary_key or next((c for c in df.columns if c.lower().endswith("_id")), None)
    if pk and pk in df.columns:
        df = df.dropDuplicates([pk])

    # 2. Fix email — replace broken '#' separator and null sentinels
    if "email" in df.columns:
        df = df.withColumn(
            "email",
            F.when(F.col("email").isin(DB_NULL_SENTINEL, ""), None)
             .otherwise(F.regexp_replace(F.col("email"), EMAIL_BROKEN_CHAR, "@")),
        )

    # 3. Normalise amount to absolute double
    if "amount" in df.columns:
        df = df.withColumn("amount", F.abs(F.col("amount").cast("double")))

    # 4. Normalise price to absolute double
    if "price" in df.columns:
        df = df.withColumn("price", F.abs(F.col("price").cast("double")))

    # 5. Cast date/time columns to timestamp
    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            df = df.withColumn(
                col,
                F.when(F.col(col).isin(DB_NULL_SENTINEL, ""), None)
                 .otherwise(F.to_timestamp(F.col(col))),
            )

    return df


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Per-table ETL
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def etl_table(spark, table: str, target_jdbc: str, pg_user: str, pg_pass: str) -> int:
    from pyspark.sql import functions as F

    logger.info("── %s ──────────────────────────────────────", table)

    # Read from MariaDB
    df = (
        spark.read.format("jdbc")
        .option("url",      SOURCE_JDBC)
        .option("dbtable",  table)
        .option("driver",   MARIADB_DRIVER)
        .option("user",     MARIADB_USER)
        .option("password", MARIADB_PASS)
        .load()
    )

    # Explicit select — makes every column a named AttributeReference in Spline
    df = df.select([F.col(c) for c in df.columns])
    logger.info("  Read %s columns from MariaDB", len(df.columns))

    # Detect primary key from column names
    pk = next((c for c in df.columns if c.lower() == f"{table[:-1]}_id"
               or c.lower() == f"{table}_id"
               or c.lower().endswith("_id")), None)

    # Apply transformations
    df = transform(df, primary_key=pk)

    # Explicit final select — Spline maps each output col back to its source col
    df = df.select([F.col(c) for c in df.columns])

    row_count = df.count()

    # Write to PostgreSQL (company_data DB)
    (
        df.write.format("jdbc")
        .option("url",      target_jdbc)
        .option("dbtable",  table)
        .option("driver",   POSTGRES_DRIVER)
        .option("user",     pg_user)
        .option("password", pg_pass)
        .mode("overwrite")
        .save()
    )

    logger.info("  Written %d rows → PostgreSQL company_data.%s", row_count, table)
    return row_count


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    logger.info("═══════════════════════════════════════════════")
    logger.info("  DataGuard ETL  —  MariaDB → PostgreSQL")
    logger.info("  Source : %s  (%s)", SOURCE_JDBC, MARIADB_DATABASE)
    logger.info("  Tables : %s", ", ".join(TABLES))
    logger.info("  Spline : %s", SPLINE_PRODUCER_URL)
    logger.info("═══════════════════════════════════════════════")

    # Ensure company_data DB exists in PostgreSQL
    try:
        from sqlalchemy import create_engine, text
        pg_base = POSTGRES_URL.rsplit("/", 1)[0] + "/postgres"
        eng = create_engine(pg_base, isolation_level="AUTOCOMMIT")
        with eng.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": TARGET_DB_NAME},
            ).fetchone()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{TARGET_DB_NAME}"'))
                logger.info("Created target database '%s'", TARGET_DB_NAME)
        eng.dispose()
    except Exception as e:
        logger.warning("Could not ensure target DB exists: %s", e)

    target_jdbc, pg_user, pg_pass = _pg_jdbc()

    spark = create_spark_session("DataGuard ETL: governance_db")

    results: dict[str, int] = {}
    errors:  list[str] = []

    for table in TABLES:
        try:
            results[table] = etl_table(spark, table, target_jdbc, pg_user, pg_pass)
        except Exception as exc:
            logger.error("ETL failed for '%s': %s", table, exc, exc_info=True)
            errors.append(f"{table}: {exc}")

    spark.stop()

    logger.info("═══════════════════════════════════════════════")
    logger.info("  ETL Complete")
    for tbl, rows in results.items():
        logger.info("  ✓ %-15s  %d rows", tbl, rows)
    for err in errors:
        logger.error("  ✗ %s", err)
    logger.info("  Lineage → Spline UI: http://localhost:9090")
    logger.info("═══════════════════════════════════════════════")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
