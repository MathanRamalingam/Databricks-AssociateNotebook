# Databricks notebook source
# MAGIC %run ../includes/Copy-Datasets

# COMMAND ----------

# MAGIC %python
# MAGIC dataset_bookstore

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing the sql query using the json to extract the data from file

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json. `${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## using the text command instead of json. it should work as normal 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from text. `${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## using the binaryfile command to get the metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from binaryfile. `${dataset.bookstore}/customers-json/export_001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## writing the sql query using the csv to extract the data from file. It will create a bad result as csv doesn't have a schema in place so we have to follow different approach.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv. `${dataset.bookstore}/books-csv`

# COMMAND ----------

# MAGIC %md
# MAGIC ## creating a table against the csv file and it qill create a external table with csv format not delta table. we need to refresh if there are any append or other activity happens after the creation of table. if the table is big then its a costly operation

# COMMAND ----------

# MAGIC %sql
# MAGIC create table books_csv1
# MAGIC (book_id string , title string , author string , category string , price double)
# MAGIC using csv
# MAGIC options ( header = "true", delimiter= ";")
# MAGIC location "${dataset.bokstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table books_csv1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_csv
# MAGIC   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC )
# MAGIC LOCATION "${dataset.bookstore}/books-csv"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_csv

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## importing extra rows to see if we get the value without refresh and it wont work without refresh

# COMMAND ----------

# MAGIC  %python
# MAGIC (spark.read
# MAGIC          .table("books_csv")
# MAGIC        .write
# MAGIC          .mode("append")
# MAGIC          .format("csv")
# MAGIC          .option('header', 'true')
# MAGIC          .option('delimiter', ';')
# MAGIC          .save(f"{dataset_bookstore}/books-csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## it shows 12 records which is before any new insert, lets refresh the table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## refreshing

# COMMAND ----------

# MAGIC %sql
# MAGIC refresh table books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## now we have 24 rows . so we should go with delta tables rather than this approach

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## works normal for json as it has schema in it 

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE customers AS
# MAGIC SELECT * FROM json.`${dataset.bookstore}/customers-json`;
# MAGIC
# MAGIC DESCRIBE EXTENDED customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ##works bad for csv

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE books_unparsed AS
# MAGIC SELECT * FROM csv.`${dataset.bookstore}/books-csv`;
# MAGIC
# MAGIC SELECT * FROM books_unparsed;

# COMMAND ----------

# MAGIC %md
# MAGIC ## new approach creating a view and then creating a ctas from that view. it will create a delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW books_tmp_vw
# MAGIC    (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "${dataset.bookstore}/books-csv/export_*.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = ";"
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE books AS
# MAGIC   SELECT * FROM books_tmp_vw;
# MAGIC   
# MAGIC SELECT * FROM books

# COMMAND ----------


