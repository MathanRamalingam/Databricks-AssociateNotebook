-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## creating managed tables

-- COMMAND ----------

CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT)


-- COMMAND ----------

describe extended  managed_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## create extended table

-- COMMAND ----------

CREATE TABLE external_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_default';
  
INSERT INTO external_default
VALUES (3 INT, 2 INT, 1 INT)

-- COMMAND ----------

describe extended external_default

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## lets drop both and see the external table content will be available in storage but managed table

-- COMMAND ----------

drop table managed_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default'

-- COMMAND ----------

drop table external_default

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

-- COMMAND ----------

create schema new_default

-- COMMAND ----------

describe database extended new_default

-- COMMAND ----------

USE new_default;

CREATE TABLE managed_new_default
  (width INT, length INT, height INT);
  
INSERT INTO managed_new_default
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_new_default
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_new_default';
  
INSERT INTO external_new_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

DROP TABLE managed_new_default;
DROP TABLE external_new_default;

-- COMMAND ----------


CREATE SCHEMA custom
LOCATION 'dbfs:/Shared/schemas/custom.db'


-- COMMAND ----------


USE custom;

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-----------------------------------

CREATE TABLE external_custom
  (width INT, length INT, height INT)
LOCATION 'dbfs:/mnt/demo/external_custom';
  
INSERT INTO external_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

describe database extended custom

-- COMMAND ----------

describe extended managed_custom

-- COMMAND ----------

describe extended external_custom

-- COMMAND ----------


-- COMMAND ----------

DROP TABLE managed_custom;
DROP TABLE external_custom;

-- COMMAND ----------

-- MAGIC  %fs ls 'dbfs:/Shared/schemas/custom.db/managed_custom'

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'

-- COMMAND ----------

CREATE TABLE managed_custom
  (width INT, length INT, height INT);
  
INSERT INTO managed_custom
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

create table x as select *, 'hello' from managed_custom

-- COMMAND ----------

select * from x;

-- COMMAND ----------

create schema custom;
CREATE TABLE managed1
  (width INT, length INT, height INT , dataa date);
  
INSERT INTO managed1
VALUES (3 INT, 2 INT, 1 INT , '2022-01-01');


-- COMMAND ----------

select * from managed1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##  adding constriant

-- COMMAND ----------

alter table managed1 alter column height set not null

-- COMMAND ----------

describe detail managed1

-- COMMAND ----------

alter table managed1 add  constraint dataa1 check  (dataa > '2000-01-01')

-- COMMAND ----------

show tblproperties managed1

-- COMMAND ----------

alter table managed1 drop constraint dataa1

-- COMMAND ----------

show tblproperties managed1

-- COMMAND ----------

alter table managed1 alter column height drop not null

-- COMMAND ----------

select * from managed1

-- COMMAND ----------

INSERT INTO managed1
VALUES (4, 9 , null , '1990-01-01');

-- COMMAND ----------

select * from managed1

-- COMMAND ----------

create table deeptab
deep clone managed1

-- COMMAND ----------

describe detail deeptab

-- COMMAND ----------

create table shtab
shallow clone managed1

-- COMMAND ----------

describe detail shtab

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/custom.db/shtab'

-- COMMAND ----------

select * from shtab

-- COMMAND ----------

INSERT INTO managed1
VALUES (4, 9 , null , '1990-01-01');

-- COMMAND ----------

select * from deeptab

-- COMMAND ----------

create table deeptab
deep clone managed1

-- COMMAND ----------


