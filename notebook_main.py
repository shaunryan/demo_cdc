# Databricks notebook source
# MAGIC %pip install yetl-framework

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
# MAGIC select * from demo_cdc_raw.customer_details_history

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from demo_cdc_raw.customer_details

# COMMAND ----------

dbutils.notebook.exit("Done")

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
