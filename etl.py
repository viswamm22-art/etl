from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs, regexp_replace, to_date, sum as spark_sum, count as spark_count

# Initialize Spark with Spline
spark = (SparkSession.builder
    .appName("Orders Transactions ETL")
    .config("spark.sql.queryExecutionListeners", "za.co.absa.spline.harvester.listener.SplineQueryExecutionListener")
    .config("spline.producer.url", "http://localhost:8080/producer")
    .getOrCreate())

# Read orders
orders = (spark.read.option("header", True)
          .csv("data/raw_orders.csv"))
orders = orders.dropDuplicates(["order_id"])
orders = orders.withColumn("email", regexp_replace(col("email"), "#", "@"))
orders = orders.withColumn("amount", abs(col("amount").cast("double")))
orders = orders.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# Read transactions
transactions = (spark.read.option("header", True)
               .csv("data/raw_transactions.csv"))
transactions = transactions.dropDuplicates(["transaction_id"])
transactions = transactions.withColumn("amount", abs(col("amount").cast("double")))
transactions = transactions.withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd"))

# Join orders with transactions
order_transactions = orders.join(transactions, on="order_id", how="inner")

# Aggregate payment summary
payment_summary = (order_transactions.groupBy("payment_method")
                   .agg(spark_sum("amount").alias("total_amount"),
                        spark_count("transaction_id").alias("transaction_count")))

# Write outputs
orders.write.mode("overwrite").parquet("output/orders")
transactions.write.mode("overwrite").parquet("output/transactions")
order_transactions.write.mode("overwrite").parquet("output/order_transactions")
payment_summary.write.mode("overwrite").parquet("output/payment_summary")

print("ETL completed successfully.")
