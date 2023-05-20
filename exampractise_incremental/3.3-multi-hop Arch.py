# Databricks notebook source
# MAGIC %run ../includes/Copy-Datasets

# COMMAND ----------

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudfiles.format","parquet")
    .option("cloudFiles.schemaLocation","dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view orders_temp as (
# MAGIC   select *, current_timestamp() as arrival_time , input_file_name() as source_file
# MAGIC   from orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_temp

# COMMAND ----------

(spark.table("orders_temp")
.writeStream
.format("delta")
.option("checkpointLocation","dbfs:/nt/demo/checkoints/orders_bronze")
.outputMode("append")
.table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_bronze

# COMMAND ----------

load_new_data()

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))


# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC    SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC           cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

load_new_data()

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC  %sql
# MAGIC  CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC    SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC    FROM orders_silver_tmp
# MAGIC  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from daily_customer_books

# COMMAND ----------


for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


