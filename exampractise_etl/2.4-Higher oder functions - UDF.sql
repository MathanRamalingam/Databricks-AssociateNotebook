-- Databricks notebook source
-- MAGIC %run ../includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select * from 
(select order_id , 
books,
filter(books, i ->  i.quantity >=2 ) as multiple_copies
from orders)
where size(multiple_copies) > 0

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select * from 
(select order_id , books ,
transform(books, b -> cast(b.subtotal * 0.8 as int) ) as subtotal_after_discount
from orders)

-- COMMAND ----------

create or replace function get_url(email string)
returns string

return concat("https://www.",split(email, "@")[1])

-- COMMAND ----------

select email, get_url(email) from customers

-- COMMAND ----------

select * from orders

-- COMMAND ----------


