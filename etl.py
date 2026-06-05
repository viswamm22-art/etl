from __future__ import annotations

import os
import sys
from urllib.parse import quote_plus

# ── Spline: must open Java module before any SparkSession is created ──────────
_SPLINE_OPTS = "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
existing = os.environ.get("JDK_JAVA_OPTIONS", "")
if _SPLINE_OPTS not in existing:
    os.environ["JDK_JAVA_OPTIONS"] = f"{existing} {_SPLINE_OPTS}".strip()

# ── Connection settings ───────────────────────────────────────────────────────
# MariaDB is exposed on the HOST at port 3307 (Docker: 3307:3306)
MARIADB_HOST = os.getenv("MARIADB_HOST",     "127.0.0.1")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", "3307"))
MARIADB_DB   = os.getenv("MARIADB_DB",       "governance_db")
MARIADB_USER = os.getenv("MARIADB_USER",     "root")
MARIADB_PASS = os.getenv("MARIADB_PASS",     "root123")

# PostgreSQL is the Docker container on port 5432
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://myuser:mypassword@localhost:5432/company_data",
)

SPLINE_PRODUCER_URL = os.getenv("SPLINE_PRODUCER_URL", "http://localhost:8080/producer")

# Tables to migrate (add/remove as needed)
TABLES = ["customers", "orders", "products", "transactions"]

# ── JDBC URL helpers ──────────────────────────────────────────────────────────

def _maria_jdbc(table: str = "") -> str:
    base = f"jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}"
    return f"{base}/{table}" if table else base


def _pg_jdbc() -> str:
    """Convert postgresql:// URL to jdbc:postgresql://"""
    url = POSTGRES_URL
    # postgresql:// → jdbc:postgresql://
    if url.startswith("postgresql://"):
        url = "jdbc:postgresql://" + url[len("postgresql://"):]
    elif not url.startswith("jdbc:"):
        url = "jdbc:postgresql://" + url
    # Strip /dataguard → replace with /company_data
    parts = url.rsplit("/", 1)
    return f"{parts[0]}/company_data" if len(parts) == 2 else url


def _pg_user_pass() -> tuple[str, str]:
    raw = POSTGRES_URL
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    creds, _ = raw.split("@", 1)
    user, pw = (creds.split(":", 1) + [""])[:2]
    return user, pw


# ── Data cleaning ─────────────────────────────────────────────────────────────

def transform(df, primary_key: str | None = None):
    """
    Clean the DataFrame before loading:
      - Deduplicate on primary key
      - Fix emails (# → @, null sentinels)
      - abs(amount) and abs(price)
      - to_timestamp for date/time columns
    """
    from pyspark.sql import functions as F

    # Deduplicate
    if primary_key and primary_key in df.columns:
        df = df.dropDuplicates([primary_key])
    else:
        candidates = [c for c in df.columns if c.lower().endswith("_id")]
        if candidates:
            df = df.dropDuplicates([candidates[0]])

    if "email" in df.columns:
        df = df.withColumn(
            "email",
            F.when(F.col("email").isin("(NULL)", "", "null"), None)
            .otherwise(F.regexp_replace(F.col("email"), "#", "@")),
        )

    if "amount" in df.columns:
        df = df.withColumn("amount", F.abs(F.col("amount").cast("double")))

    if "price" in df.columns:
        df = df.withColumn("price", F.abs(F.col("price").cast("double")))

    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            df = df.withColumn(
                col,
                F.when(F.col(col).isin("(NULL)", ""), None)
                .otherwise(F.to_timestamp(F.col(col))),
            )

    return df


# ── ETL per table ─────────────────────────────────────────────────────────────

def run_table(table: str, spark, source_jdbc: str, target_jdbc: str,
              src_user: str, src_pass: str, tgt_user: str, tgt_pass: str) -> int:
    from pyspark.sql import functions as F

    df = (
        spark.read.format("jdbc")
        .option("url",      source_jdbc)
        .option("dbtable",  table)
        .option("driver",   "org.mariadb.jdbc.Driver")
        .option("user",     src_user)
        .option("password", src_pass)
        .load()
    )

    # Explicit select → Spline registers named AttributeReferences for column lineage
    src_cols = df.columns
    df = df.select([F.col(c) for c in src_cols])

    df = transform(df)

    out_cols = df.columns
    df = df.select([F.col(c) for c in out_cols])

    count = df.count()

    (
        df.write.format("jdbc")
        .option("url",      target_jdbc)
        .option("dbtable",  table)
        .option("driver",   "org.postgresql.Driver")
        .option("user",     tgt_user)
        .option("password", tgt_pass)
        .mode("overwrite")
        .save()
    )

    print(f"[OK] {table}: {count} rows")
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    from pyspark.sql import SparkSession

    packages = ",".join([
        "za.co.absa.spline.agent.spark:spark-3.5-spline-agent-bundle_2.12:2.2.1",
        "org.mariadb.jdbc:mariadb-java-client:3.3.3",
        "org.postgresql:postgresql:42.7.3",
    ])

    existing_session = SparkSession.getActiveSession()
    if existing_session:
        existing_session.stop()

    spark = (
        SparkSession.builder
        .appName("DataGuard ETL")
        .master("local[*]")
        .config("spark.jars.packages",            packages)
        .config("spark.spline.producer.url",      SPLINE_PRODUCER_URL)
        .config("spark.spline.mode",              "ENABLED")
        .config("spark.ui.enabled",               "false")
        .config("spark.sql.shuffle.partitions",   "4")
        .getOrCreate()
    )

    # Activate Spline Agent via JVM bridge
    init_cls = getattr(spark._jvm, "za.co.absa.spline.harvester.SparkLineageInitializer$")
    init_cls.MODULE$.enableLineageTracking(spark._jsparkSession)
    print("[OK] Spline Agent active")

    source_jdbc = _maria_jdbc()
    target_jdbc = _pg_jdbc()
    tgt_user, tgt_pass = _pg_user_pass()

    print(f"Source: {source_jdbc}")
    print(f"Target: {target_jdbc}")

    total = 0
    errors = []
    for table in TABLES:
        try:
            total += run_table(
                table, spark,
                source_jdbc, target_jdbc,
                MARIADB_USER, MARIADB_PASS,
                tgt_user, tgt_pass,
            )
        except Exception as exc:
            print(f"[ERROR] {table}: {exc}", file=sys.stderr)
            errors.append(table)

    spark.stop()

    if errors:
        print(f"[WARN] Failed tables: {errors}", file=sys.stderr)
        sys.exit(1 if len(errors) == len(TABLES) else 0)

    print(f"[DONE] {len(TABLES) - len(errors)} tables, {total} total rows")


if __name__ == "__main__":
    main()
