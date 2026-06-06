"""
etl_pipeline.py — PySpark ETL: MariaDB → PostgreSQL with Spline lineage.

Upload this file to your GitHub repo as etl.py (or whatever path you configured).
All connection values are read from environment variables injected by the pipeline server.
"""
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl")

# ── Config from environment (injected by DataGuard pipeline server) ───────────
MARIADB_HOST = os.getenv("MARIADB_HOST", "127.0.0.1")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", "3307"))
MARIADB_DB   = os.getenv("MARIADB_DB",   "governance_db")
MARIADB_USER = os.getenv("MARIADB_USER", "root")
MARIADB_PASS = os.getenv("MARIADB_PASS", "root123")

POSTGRES_URL        = os.getenv("POSTGRES_URL", "postgresql://myuser:mypassword@localhost:5432/dataguard")
SPLINE_PRODUCER_URL = os.getenv("SPLINE_PRODUCER_URL", "http://localhost:8080/producer")

SPARK_MASTER       = os.getenv("SPARK_MASTER", "local[*]")
SPARK_PARTITIONS   = os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")
SPLINE_AGENT_VER   = os.getenv("SPLINE_AGENT_VERSION", "2.2.1")
MARIADB_JAR_VER    = os.getenv("MARIADB_DRIVER_VERSION", "3.3.3")
POSTGRES_JAR_VER   = os.getenv("POSTGRES_DRIVER_VERSION", "42.7.3")

# ── Parse PostgreSQL URL → JDBC parts ─────────────────────────────────────────
import urllib.parse
_p = urllib.parse.urlparse(POSTGRES_URL)
PG_USER = _p.username or "myuser"
PG_PASS = _p.password or "mypassword"
PG_HOST = _p.hostname or "localhost"
PG_PORT = _p.port or 5432
TARGET_DB = os.getenv("TARGET_DB_NAME", "company_data")

SOURCE_JDBC = f"jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}"
TARGET_JDBC = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{TARGET_DB}"

# ── Java 17 fix ───────────────────────────────────────────────────────────────
_JAVA_OPENS = "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
if _JAVA_OPENS not in os.environ.get("JDK_JAVA_OPTIONS", ""):
    os.environ["JDK_JAVA_OPTIONS"] = f"{os.environ.get('JDK_JAVA_OPTIONS','').strip()} {_JAVA_OPENS}".strip()


def create_spark_session():
    from pyspark.sql import SparkSession

    packages = ",".join([
        f"za.co.absa.spline.agent.spark:spark-3.5-spline-agent-bundle_2.12:{SPLINE_AGENT_VER}",
        f"org.mariadb.jdbc:mariadb-java-client:{MARIADB_JAR_VER}",
        f"org.postgresql:postgresql:{POSTGRES_JAR_VER}",
    ])

    existing = SparkSession.getActiveSession()
    if existing:
        existing.stop()

    spark = (
        SparkSession.builder
        .appName("DataGuard ETL")
        .master(SPARK_MASTER)
        .config("spark.jars.packages",          packages)
        .config("spark.spline.producer.url",    SPLINE_PRODUCER_URL)
        .config("spark.spline.mode",            "ENABLED")
        .config("spark.ui.enabled",             "false")
        .config("spark.sql.shuffle.partitions", SPARK_PARTITIONS)
        .getOrCreate()
    )

    # Activate Spline Agent — MODULE$ is Scala companion object syntax
    init_cls = getattr(spark._jvm, "za.co.absa.spline.harvester.SparkLineageInitializer$")
    getattr(init_cls, "MODULE$").enableLineageTracking(spark._jsparkSession)
    logger.info("Spline Agent active")
    return spark


def clean_dataframe(df, primary_key=None):
    from pyspark.sql import functions as F

    pk = primary_key or next((c for c in df.columns if c.lower().endswith("_id")), None)
    if pk and pk in df.columns:
        df = df.dropDuplicates([pk])

    if "email" in df.columns:
        df = df.withColumn("email",
            F.when(F.col("email").isin("(NULL)", ""), None)
             .otherwise(F.regexp_replace(F.col("email"), "#", "@"))
        )

    if "amount" in df.columns:
        df = df.withColumn("amount", F.abs(F.col("amount").cast("double")))
    if "price" in df.columns:
        df = df.withColumn("price", F.abs(F.col("price").cast("double")))

    for col in df.columns:
        if "date" in col.lower() or "time" in col.lower():
            df = df.withColumn(col,
                F.when(F.col(col).isin("(NULL)", ""), None)
                 .otherwise(F.to_timestamp(F.col(col)))
            )

    return df


def run_table_etl(spark, table, primary_key=None):
    from pyspark.sql import functions as F

    logger.info("Reading table: %s", table)
    df = (
        spark.read.format("jdbc")
        .option("url",      SOURCE_JDBC)
        .option("dbtable",  table)
        .option("driver",   "org.mariadb.jdbc.Driver")
        .option("user",     MARIADB_USER)
        .option("password", MARIADB_PASS)
        .load()
    )

    df = df.select([F.col(c) for c in df.columns])
    df = clean_dataframe(df, primary_key=primary_key)
    df = df.select([F.col(c) for c in df.columns])

    rows = df.count()

    (
        df.write.format("jdbc")
        .option("url",      TARGET_JDBC)
        .option("dbtable",  table)
        .option("driver",   "org.postgresql.Driver")
        .option("user",     PG_USER)
        .option("password", PG_PASS)
        .mode("overwrite")
        .save()
    )

    logger.info("Done: %s — %d rows", table, rows)
    return rows


def get_tables():
    import pymysql
    conn = pymysql.connect(
        host=MARIADB_HOST, port=MARIADB_PORT,
        user=MARIADB_USER, password=MARIADB_PASS,
        database=MARIADB_DB, connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SHOW TABLES")
        tables = [r[0] for r in cur.fetchall()]
    conn.close()
    return tables


def main():
    logger.info("Source: %s", SOURCE_JDBC)
    logger.info("Target: %s", TARGET_JDBC)

    tables = get_tables()
    if not tables:
        logger.error("No tables found in %s", MARIADB_DB)
        sys.exit(1)

    logger.info("Tables: %s", tables)
    spark = create_spark_session()

    failed = []
    for table in tables:
        try:
            rows = run_table_etl(spark, table)
            logger.info("[OK] %s: %d rows", table, rows)
        except Exception as exc:
            logger.error("[FAIL] %s: %s", table, exc)
            failed.append(table)

    spark.stop()

    if failed:
        logger.warning("[WARN] Failed tables: %s", failed)
        sys.exit(1)

    logger.info("ETL complete — all %d tables transferred", len(tables))


if __name__ == "__main__":
    main()
