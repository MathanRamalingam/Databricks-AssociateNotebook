# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook basics

# COMMAND ----------

# MAGIC %md
# MAGIC ##calling another notebook via %run magic

# COMMAND ----------

# MAGIC %run ../includes/setup

# COMMAND ----------

print(name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Databricks dataset via %fs magic

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils 

# COMMAND ----------

dbutils.help()

# COMMAND ----------

files = dbutils.fs.ls('databricks-datasets/')

# COMMAND ----------

display(files)
