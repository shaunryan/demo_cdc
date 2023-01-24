# Databricks notebook source
# MAGIC %pip install yetl-framework

# COMMAND ----------

def clear_down():
  try:
    spark.sql("drop database demo_cdc_landing cascade")
  except:
    pass
  try:
    spark.sql("drop database demo_cdc_raw cascade")
  except:
    pass
  dbutils.fs.rm("/mnt/datalake/data/delta_lake/demo_cdc_landing/", True)
  dbutils.fs.rm("/mnt/datalake/data/delta_lake/demo_cdc_raw/", True)

clear_down()

# COMMAND ----------

from src.demo_landing_to_raw import landing_to_raw
from yetl.flow import Timeslice, OverwriteSave
from yetl.workflow import multithreaded as yetl_wf
import yaml

timeslice = Timeslice(year="*", month="*", day="*")
project = "demo_cdc"
maxparallel = 2

path = f"./config/project/{project}/{project}_tables.yml"

with open(path, "r", encoding="utf-8") as f:
    metdata = yaml.safe_load(f)

tables: list = [t["name"] for t in metdata.get("tables")]

yetl_wf.load(project, tables, landing_to_raw, timeslice, OverwriteSave, maxparallel)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS demo_cdc_raw.customer_details_hist
# MAGIC (
# MAGIC 	`key` integer,
# MAGIC   `flag` string  ,
# MAGIC 	`id` int  ,
# MAGIC 	`first_name` string  ,
# MAGIC 	`last_name` string  ,
# MAGIC 	`email` string  ,
# MAGIC 	`gender` string  ,
# MAGIC 	`job_title` string  ,
# MAGIC 	`amount` double  ,
# MAGIC 	`_timeslice` timestamp  ,
# MAGIC 	`_filepath_filename` string NOT NULL ,
# MAGIC 	`_dataset_id` string NOT NULL 
# MAGIC )
# MAGIC USING DELTA LOCATION '/mnt/datalake/data/delta_lake/demo_cdc_raw/customer_details_hist'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO demo_cdc_raw.customer_details_hist
# MAGIC (
# MAGIC 	`key`,
# MAGIC   `flag` ,
# MAGIC 	`id`,
# MAGIC 	`first_name` ,
# MAGIC 	`last_name`,
# MAGIC 	`email`,
# MAGIC 	`gender` ,
# MAGIC 	`job_title`,
# MAGIC 	`amount`,
# MAGIC 	`_timeslice`,
# MAGIC 	`_filepath_filename`,
# MAGIC 	`_dataset_id`
# MAGIC )
# MAGIC select
# MAGIC 	`key`,
# MAGIC   `flag` ,
# MAGIC 	`id`,
# MAGIC 	`first_name` ,
# MAGIC 	`last_name`,
# MAGIC 	`email`,
# MAGIC 	`gender` ,
# MAGIC 	`job_title`,
# MAGIC 	`amount`,
# MAGIC 	`_timeslice`,
# MAGIC 	`_filepath_filename`,
# MAGIC 	`_dataset_id`
# MAGIC from (
# MAGIC 	SELECT
# MAGIC 		row_number() over (partition by id order by _timeslice desc) as _is_latest,
# MAGIC 		*
# MAGIC 	FROM demo_cdc_raw.customer_details
# MAGIC ) latest
# MAGIC WHERE _is_latest != 1 OR flag = 'D' 
# MAGIC -- QAULIFY row_number() over (partition by id order by _timeslice desc)  == 1

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC delete 
# MAGIC -- select *
# MAGIC from demo_cdc_raw.customer_details c
# MAGIC where sha2(concat_ws('|',id, _timeslice, flag), 0) in (
# MAGIC   select
# MAGIC     sha2(concat_ws('|',id, _timeslice, flag), 0)
# MAGIC   from demo_cdc_raw.customer_details_hist h
# MAGIC )

# COMMAND ----------

current_count = spark.sql("select count(*) from demo_cdc_raw.customer_details").collect()[0][0]
distinct_current_count = spark.sql("select count(distinct id) from demo_cdc_raw.customer_details").collect()[0][0]

assert current_count == 19, f"current_count={current_count}"
assert current_count == distinct_current_count, f"distinct_current_count={distinct_current_count}"

# COMMAND ----------

hist_count = spark.sql("select count(*) from demo_cdc_raw.customer_details_hist").collect()[0][0]
assert hist_count == 13, f"hist_count={hist_count}"

# COMMAND ----------

history_count = spark.sql("select count(*) from demo_cdc_raw.customer_details_history").collect()[0][0]
assert history_count == 20, f"hist_count={history_count}"


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace view demo_cdc_raw.v_customer_details as
# MAGIC 
# MAGIC select
# MAGIC   `key`,
# MAGIC   _source,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   _end_date,
# MAGIC   if (end_date = '9999-12-31', 'Y', 'N') as is_current,
# MAGIC   `id`,
# MAGIC   `first_name` ,
# MAGIC   `last_name`,
# MAGIC   `email`,
# MAGIC   `gender` ,
# MAGIC   `job_title`,
# MAGIC   `amount`,
# MAGIC   _is_history_migration
# MAGIC from
# MAGIC (
# MAGIC   select
# MAGIC       `key`,
# MAGIC       _source,
# MAGIC       start_date,
# MAGIC       case _is_history_migration
# MAGIC         when end_date < cast('9999-12-31' as Date) then end_date
# MAGIC         else
# MAGIC           lag(start_date, 1, cast('9999-12-31' as Date)) over (PARTITION by id order by start_date desc, _priority)
# MAGIC       end as end_date,
# MAGIC       end_date as _end_date,
# MAGIC       is_current,
# MAGIC       `id`,
# MAGIC       `first_name` ,
# MAGIC       `last_name`,
# MAGIC       `email`,
# MAGIC       `gender` ,
# MAGIC       `job_title`,
# MAGIC       `amount`,
# MAGIC       _is_history_migration
# MAGIC   from
# MAGIC   (
# MAGIC     select
# MAGIC       `key`,
# MAGIC       'customer_details' as _source,
# MAGIC       cast(_timeslice as date) as start_date,
# MAGIC       cast(null as date) as end_date,
# MAGIC       'Y' as is_current,
# MAGIC       `id`,
# MAGIC       `first_name` ,
# MAGIC       `last_name`,
# MAGIC       `email`,
# MAGIC       `gender` ,
# MAGIC       `job_title`,
# MAGIC       `amount`,
# MAGIC       False as _is_history_migration,
# MAGIC       1 as _priority
# MAGIC     from demo_cdc_raw.customer_details
# MAGIC     union all
# MAGIC     select
# MAGIC       `key`,
# MAGIC       'customer_details_hist' as _source,
# MAGIC       cast(_timeslice as date) as start_date,
# MAGIC       cast(null as date) as end_date,
# MAGIC       'Y' as is_current,
# MAGIC       `id`,
# MAGIC       `first_name` ,
# MAGIC       `last_name`,
# MAGIC       `email`,
# MAGIC       `gender` ,
# MAGIC       `job_title`,
# MAGIC       `amount`,
# MAGIC       False as _is_history_migration,
# MAGIC       2 as _priority
# MAGIC     from demo_cdc_raw.customer_details_hist
# MAGIC     union all
# MAGIC     select
# MAGIC       `key`,
# MAGIC       'customer_details_history' as _source,
# MAGIC       start_date,
# MAGIC       end_date,
# MAGIC       is_current,
# MAGIC       `id`,
# MAGIC       `first_name` ,
# MAGIC       `last_name`,
# MAGIC       `email`,
# MAGIC       `gender` ,
# MAGIC       `job_title`,
# MAGIC       `amount`,
# MAGIC       True as _is_history_migration,
# MAGIC       3 as _priority
# MAGIC     from demo_cdc_raw.customer_details_history
# MAGIC   ) all
# MAGIC ) with_latest
# MAGIC where not (_source = 'customer_details_history' and start_date = end_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from demo_cdc_raw.v_customer_details
# MAGIC where id = 4
# MAGIC order by `id` --, start_date

# COMMAND ----------

from datetime import datetime
# .strftime('Y%-%m-d%')
snapshots = spark.sql("select distinct start_date from demo_cdc_raw.v_customer_details").collect()
snapshots = [r.start_date.strftime('%Y-%m-%d') for r in snapshots]
snapshots = snapshots + [ '2022-08-13']


# COMMAND ----------


from pyspark.sql import DataFrame

sql = f"""
SELECT 
  cast('[snapshot_date]' as date) snapshot_date,
  *
FROM demo_cdc_raw.v_customer_details
WHERE cast('[snapshot_date]' as date) >= start_date and cast('[snapshot_date]' as date) < end_date
"""

df_all:DataFrame = None
for s in snapshots:
  snapshot_sql = sql.replace('[snapshot_date]', s)
  print(snapshot_sql)
  df = spark.sql(snapshot_sql)

  if df_all:
    df_all = df_all.union(df)
  else:
    df_all = df




# COMMAND ----------

result = (df_all
  .orderBy(['snapshot_date','id'])
  .write.format("delta")
  .mode("Overwrite")
  .saveAsTable("demo_cdc_raw.customer_details_snapshot", path='/mnt/datalake/data/delta_lake/demo_cdc_raw/customer_details_snapshot')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select distinct snapshot_date from demo_cdc_raw.customer_details_snapshot order by snapshot_date

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select *
# MAGIC from demo_cdc_raw.customer_details_snapshot
# MAGIC where snapshot_date = cast('2022-08-13' as date)
