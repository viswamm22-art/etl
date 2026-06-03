```python
import os
from sqlalchemy import create_engine
from lineage_tracker import LineageTracker

# --------------------------------------------------
# Database
# --------------------------------------------------

engine = create_engine(
    "mysql+pymysql://root:password@localhost:3307/governance_db"
)

# --------------------------------------------------
# Tracker
# --------------------------------------------------

tracker = LineageTracker(
    job_name="Orders Transactions ETL"
)

# --------------------------------------------------
# Helper
# --------------------------------------------------

def clean_email(email):
    if email is None:
        return None

    email = str(email)

    if email == "(NULL)":
        return None

    email = email.replace("#", "@")

    if "@" not in email:
        if "gmail.com" in email:
            email = email.replace(
                "gmail.com",
                "@gmail.com"
            )

    return email


# --------------------------------------------------
# ORDERS
# --------------------------------------------------

orders = tracker.read_csv(
    "data/raw_orders.csv",
    source_uri="github://etl-demo/data/raw_orders.csv",
    source_dataset="raw_orders"
)

orders = tracker.drop_duplicates(
    orders,
    subset=["order_id"]
)

orders["email"] = tracker.apply_column(
    orders,
    "email",
    clean_email,
    "clean_email"
)

orders["amount"] = tracker.to_numeric(
    orders,
    "amount"
)

orders["amount"] = tracker.abs_column(
    orders,
    "amount"
)

orders = tracker.to_datetime(
    orders,
    ["order_date"]
)

tracker.to_sql(
    orders,
    table_name="orders",
    con=engine,
    target_uri="mariadb://governance_db/orders"
)

# --------------------------------------------------
# TRANSACTIONS
# --------------------------------------------------

transactions = tracker.read_csv(
    "data/raw_transactions.csv",
    source_uri="github://etl-demo/data/raw_transactions.csv",
    source_dataset="raw_transactions"
)

transactions = tracker.drop_duplicates(
    transactions,
    subset=["transaction_id"]
)

transactions["amount"] = tracker.to_numeric(
    transactions,
    "amount"
)

transactions["amount"] = tracker.abs_column(
    transactions,
    "amount"
)

transactions = tracker.to_datetime(
    transactions,
    ["transaction_date"]
)

tracker.to_sql(
    transactions,
    table_name="transactions",
    con=engine,
    target_uri="mariadb://governance_db/transactions"
)

# --------------------------------------------------
# JOIN
# --------------------------------------------------

order_transactions = tracker.merge_dataframes(
    orders,
    transactions,
    on="order_id",
    how="inner"
)

tracker.to_sql(
    order_transactions,
    table_name="order_transactions",
    con=engine,
    target_uri="mariadb://governance_db/order_transactions"
)

# --------------------------------------------------
# AGGREGATION
# --------------------------------------------------

payment_summary = tracker.groupby_agg(
    order_transactions,
    group_cols=["payment_method"],
    agg_dict={
        "amount": "sum"
    }
)

tracker.to_sql(
    payment_summary,
    table_name="payment_summary",
    con=engine,
    target_uri="mariadb://governance_db/payment_summary"
)

# --------------------------------------------------
# FINALIZE
# --------------------------------------------------

lineage = tracker.finalize()

print("ETL completed successfully")
print(
    f"Datasets tracked: "
    f"{len(lineage['dataset_lineage'])}"
)

print(
    f"Columns tracked: "
    f"{len(lineage['column_lineage'])}"
)
```
