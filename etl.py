import pandas as pd
from sqlalchemy import create_engine
import time

from engines.spline_engine import push_table_spline_lineage

engine = create_engine(
    "mysql+pymysql://root:password@localhost:3307/governance_db"
)

def clean_email(email):
    if pd.isna(email):
        return None

    email = str(email).replace("#", "@")
    return email

# -----------------------
# CUSTOMERS
# -----------------------

customers = pd.read_csv("data/customers.csv")

customers["email"] = customers["email"].apply(clean_email)

customers.to_sql(
    "customers",
    con=engine,
    if_exists="replace",
    index=False
)

push_table_spline_lineage(
    integration_name="GitHub ETL Demo",
    table_name="customers",
    columns=customers.columns.tolist(),
    source_uri="file://customers.csv",
    target_uri="mysql://governance_db/customers",
    duration_seconds=1
)

# -----------------------
# ORDERS
# -----------------------

orders = pd.read_csv("data/orders.csv")

orders["amount"] = (
    pd.to_numeric(
        orders["amount"],
        errors="coerce"
    )
    .fillna(0)
    .abs()
)

orders.to_sql(
    "orders",
    con=engine,
    if_exists="replace",
    index=False
)

push_table_spline_lineage(
    integration_name="GitHub ETL Demo",
    table_name="orders",
    columns=orders.columns.tolist(),
    source_uri="file://orders.csv",
    target_uri="mysql://governance_db/orders",
    duration_seconds=1
)

# -----------------------
# JOIN
# -----------------------

customer_orders = orders.merge(
    customers,
    on="customer_id",
    how="left"
)

customer_orders["total_value"] = (
    customer_orders["amount"] * 1.18
)

customer_orders.to_sql(
    "customer_orders",
    con=engine,
    if_exists="replace",
    index=False
)

push_table_spline_lineage(
    integration_name="GitHub ETL Demo",
    table_name="customer_orders",
    columns=customer_orders.columns.tolist(),
    source_uri="mysql://governance_db/orders,mysql://governance_db/customers",
    target_uri="mysql://governance_db/customer_orders",
    duration_seconds=1
)

# -----------------------
# AGGREGATION
# -----------------------

summary = (
    customer_orders
    .groupby("customer_id")
    .agg(
        total_orders=("order_id", "count"),
        total_amount=("amount", "sum")
    )
    .reset_index()
)

summary.to_sql(
    "customer_summary",
    con=engine,
    if_exists="replace",
    index=False
)

push_table_spline_lineage(
    integration_name="GitHub ETL Demo",
    table_name="customer_summary",
    columns=summary.columns.tolist(),
    source_uri="mysql://governance_db/customer_orders",
    target_uri="mysql://governance_db/customer_summary",
    duration_seconds=1
)

print("ETL completed successfully")
