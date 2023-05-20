-- Databricks notebook source
-- MAGIC %md
-- MAGIC default table is delta table

-- COMMAND ----------


CREATE TABLE employees
  (id INT, name STRING, salary DOUBLE);

-- COMMAND ----------


INSERT INTO employees
VALUES 
  (1, "Adam", 3500.0),
  (2, "Sarah", 4020.5),
  (3, "John", 2999.3),
  (4, "Thomas", 4000.3),
  (5, "Anna", 2500.0),
  (6, "Kim", 6200.3)

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

UPDATE employees 
SET salary = salary + 100
WHERE name LIKE "A%"

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employees

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

describe history employees

-- COMMAND ----------


