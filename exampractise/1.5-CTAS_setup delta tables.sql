-- Databricks notebook source
use custom;
select * from x

-- COMMAND ----------

create table y 
partitioned by (width)
comment 'hello drop'
as select * from x;

-- COMMAND ----------

select * from y;

-- COMMAND ----------

drop table managed_custom

-- COMMAND ----------

select * from x

-- COMMAND ----------

drop table x

-- COMMAND ----------

drop table  y

-- COMMAND ----------

show tables

-- COMMAND ----------

show databases

-- COMMAND ----------

drop database custom

-- COMMAND ----------


