-- Databricks notebook source
use custom;

-- COMMAND ----------

create view myview as select * from managed1

-- COMMAND ----------


create temporary view mytempview as select * from managed1

-- COMMAND ----------

select * from myview

-- COMMAND ----------

create temp view myteview as select * from managed1

-- COMMAND ----------

select * from myteview

-- COMMAND ----------

create global temporary view mygview as select * from managed1

-- COMMAND ----------

select * from global_temp.mygview

-- COMMAND ----------

create global temp view mygview1 as select * from managed1

-- COMMAND ----------

select * from global_temp.mygview

-- COMMAND ----------

drop database custom cascade

-- COMMAND ----------


