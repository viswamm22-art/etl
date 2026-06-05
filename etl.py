"""
etl_pipeline.py
═══════════════
Standalone PySpark ETL: MariaDB (governance_db) → PostgreSQL (company_data).

Upload this file to GitHub as-is. DataGuard fetches it and runs it as a
subprocess so Spline Agent captures full column-level lineage.

Tables processed: customers, orders, products, transactions
"""

from __future__ import annotations

import os
import sys

# ── Java 17: must set before any SparkSession is created ─────────────────────
_SPLINE_OPTS = "--add-opens=java.base/sun.net.www.protocol.jar=ALL-UNNAMED"
_existing = os.environ.get("JDK_JAVA_OPTIONS", "")
if _SPLINE_OPTS not in _existing:
    os.environ["JDK_JAVA_OPTIONS"] = f"{_existing} {_SPLINE_OPTS}".strip()

# ── Connection settings ───────────────────────────────────────────────────────
# MariaDB runs in Docker exposed on the HOST at port 3307 (container port 3306)
MARIADB_HOST = os.getenv("MARIADB_HOST",     "127.0.0.1")
MARIADB_PORT = int(os.getenv("MARIADB_PORT", "3307"))
MARIADB_DB   = os.getenv("MARIADB_DB",       "governance_db")
MARIADB_USER = os.getenv("MARIADB_USER",     "root")
MARIADB_PASS = os.getenv("MARIADB_PASS",     "root123")

# PostgreSQL runs in Docker exposed on port 5432
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql://myuser:mypassword@localhost:5432/dataguard",
)

SPLINE_PRODUCER_URL = os.getenv("SPLINE_PRODUCER_URL", "http://localhost:8080/producer")

TABLES = ["customers", "orders", "products", "transactions"]


# ── JDBC helpers ──────────────────────────────────────────────────────────────

def _maria_jdbc() -> str:
    return f"jdbc:mariadb://{MARIADB_HOST}:{MARIADB_PORT}/{MARIADB_DB}"


def _pg_jdbc() -> tuple[str, str, str]:
    """Return (jdbc_url, user, password) for PostgreSQL company_data DB."""
    url = POSTGRES_URL
    if url.startswith("postgresql://"):
        url = "jdbc:postgresql://" + url[len("postgresql://"):]
    elif not url.startswith("jdbc:"):
        url = "jdbc:postgresql://" + url
    # Replace DB name with company_data
    base, _ = url.rsplit("/", 1)
    jdbc_url = f"{base}/company_data"

    raw = POSTGRES_URL
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    creds_host, _ = raw.split("@", 1)
    user, pw = (creds_host.split(":", 1) + [""])[:2]
    return jdbc_url, user, pw


# ── Data cleaning ─────────────────────────────────────────────────────────────

def transform(df, primary_key=None):
    from pyspark.sql import functions as F

    # Deduplicate on primary key
    pk = primary_key
    if pk is None:
        candidates = [c for c in df.columns if c.lower().endswith("_id")]
        pk = candidates[0] if candidates else None
    if pk and pk in df.columns:
        df = df.dropDuplicates([pk])

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

def run_table(table, spark, source_jdbc, target_jdbc, tgt_user, tgt_pass):
    from pyspark.sql import functions as F

    df = (
        spark.read.format("jdbc")
        .option("url",      source_jdbc)
        .option("dbtable",  table)
        .option("driver",   "org.mariadb.jdbc.Driver")
        .option("user",     MARIADB_USER)
        .option("password", MARIADB_PASS)
        .load()
    )

    # Explicit select before transform so Spline sees named AttributeReferences
    # — this is required for column-level lineage to appear in the Spline UI
    src_cols = df.columns
    df = df.select([F.col(c) for c in src_cols])

    df = transform(df)

    # Explicit select before write so Spline maps each output column to its source
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

    print(f"[OK] {table}: {count} rows written to company_data")
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
        .config("spark.jars.packages",          packages)
        .config("spark.spline.producer.url",    SPLINE_PRODUCER_URL)
        .config("spark.spline.mode",            "ENABLED")
        .config("spark.ui.enabled",             "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # Spline 2.x: SparkLineageInitializer$ is a Scala companion object.
    # MODULE$ is the singleton instance — must use getattr() because $ is
    # not a valid Python identifier character.
    init_cls = getattr(spark._jvm, "za.co.absa.spline.harvester.SparkLineageInitializer$")
    getattr(init_cls, "MODULE$").enableLineageTracking(spark._jsparkSession)
    print("[OK] Spline Agent active — lineage will be captured on every write")

    source_jdbc = _maria_jdbc()
    target_jdbc, tgt_user, tgt_pass = _pg_jdbc()

    print(f"Source: {source_jdbc}")
    print(f"Target: {target_jdbc}")

    total_rows = 0
    failed_tables = []

    for table in TABLES:
        try:
            total_rows += run_table(table, spark, source_jdbc, target_jdbc, tgt_user, tgt_pass)
        except Exception as exc:
            print(f"[ERROR] {table}: {exc}", file=sys.stderr)
            failed_tables.append(table)

    spark.stop()

    if failed_tables:
        print(f"[WARN] Failed tables: {failed_tables}", file=sys.stderr)
        if len(failed_tables) == len(TABLES):
            sys.exit(1)

    print(f"[DONE] {len(TABLES) - len(failed_tables)} tables, {total_rows} total rows")


if __name__ == "__main__":
    main()
