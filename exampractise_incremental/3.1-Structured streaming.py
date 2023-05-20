# Databricks notebook source
# MAGIC %run ../includes/Copy-Datasets

# COMMAND ----------

(spark.readStream.table("books")).createOrReplaceTempView("books_streaming_tempview")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from books_streaming_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC select author, count(book_id) as total_books from books_streaming_tempview 
# MAGIC group by author;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view author_count as select author, count(book_id) as total_books from books_streaming_tempview 
# MAGIC group by author;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from author_count

# COMMAND ----------

(spark.table("author_count").writeStream
.trigger(processingTime='4 seconds')
.outputMode("complete")
.option("checkpointLocation", "dbfs:/mnt/demo/author_count_checkpoint")
.table("author_final"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from author_final

# COMMAND ----------

# MAGIC %sql
# MAGIC  INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC          ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_final

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC  values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC          ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC          ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from author_final

# COMMAND ----------

(
    spark.table("author_count").writeStream
.trigger(availableNow=True)
.outputMode("complete")
.option("checkpointLocation", "dbfs:/mnt/demo/author_count_checkpoint")
.table("author_final")
.awaitTermination()
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from author_final

# COMMAND ----------


