# Databricks notebook source
import re
import builtins as BI
from pyspark.sql import functions as FT

# Get all tags
def getTags() -> dict: 
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if BI.len(values) > 0:
      return values
  except:
    return defaultValue

def getLessonName() -> str:
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
username = getTag("user").lower()
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)

# The course dataset_name
dataset_name = "Spark_Hackthon"
clean_course_name = re.sub("[^a-zA-Z0-9]", "_", dataset_name)

# The path to our user's working directory. This combines both the
# username and dataset_name to create a "globally unique" folder
working_dir = f"dbfs:/user/{username}/{dataset_name}"
meta_dir = f"{working_dir}/raw/orders/batch/2017.txt"

batch_2017_path = f"{working_dir}/raw/orders/batch/2017.txt"
batch_2018_path = f"{working_dir}/raw/orders/batch/2018.csv"
batch_2019_path = f"{working_dir}/raw/orders/batch/2019.csv"
batch_target_path = f"{working_dir}/batch_orders_dirty.delta"

stream_path =                        f"{working_dir}/raw/orders/stream"
orders_checkpoint_path =             f"{working_dir}/checkpoint/orders"
line_items_checkpoint_path =         f"{working_dir}/checkpoint/line_items"

products_xsd_path = f"{working_dir}/raw/products/products.xsd"
products_xml_path = f"{working_dir}/raw/products/products.xml"

batch_source_path = batch_target_path
batch_temp_view = "batched_orders"

user_db = f"""Spark_Hackthon_{clean_username}_{clean_course_name}"""

orders_table = "orders"
sales_reps_table = "sales_reps"
products_table = "products"
#product_line_items_table = "product_line_items"
line_items_table = "line_items"

question_1_results_table = "question_1_results"
question_2_results_table = "question_2_results"
question_3_results_table = "question_3_results"

def load_meta():
  global meta, meta_batch_count_2017, meta_batch_count_2018, meta_batch_count_2019, meta_products_count, meta_orders_count, meta_line_items_count, meta_sales_reps_count, meta_stream_count, meta_ssn_format_count
  
  meta = spark.read.json(f"{working_dir}/raw/_meta/meta.json").first()
  meta_batch_count_2017 = meta["batchCount2017"]
  meta_batch_count_2018 = meta["batchCount2018"]
  meta_batch_count_2019 = meta["batchCount2019"]
  meta_products_count = meta["productsCount"]
  meta_orders_count = meta["ordersCount"]
  meta_line_items_count = meta["lineItemsCount"]
  meta_sales_reps_count = meta["salesRepsCount"]
  meta_stream_count = meta["streamCount"]
  meta_ssn_format_count = meta["ssnFormatCount"]


# COMMAND ----------

# The user's name (email address) will be used to create a home directory into which all datasets will
# be written into so as to further isolating changes from other students running the same material.
raw_data_dir = f"{working_dir}/raw"

def path_exists(path):
  try:
    return BI.len(dbutils.fs.ls(path)) >= 0
  except Exception:
    return False
  

def install_datasets(reinstall):
  source_dir = f"wasbs://courseware@dbacademy.blob.core.windows.net/developer-foundations-capstone/v01"
  
  # BI.print(f"\nThe source directory for this dataset is\n{source_dir}/\n")
  existing = path_exists(raw_data_dir)

  if not reinstall and existing:
    BI.print(f"Skipping install of existing dataset to\n{raw_data_dir}")
    return 
  
  # Remove old versions of the previously installed datasets
  if existing:
    BI.print(f"Removing previously installed datasets from\n{raw_data_dir}/")
    dbutils.fs.rm(raw_data_dir, True)

  BI.print(f"""\nInstalling the datasets to {raw_data_dir}""")
  
  # BI.print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the
  #     region that your workspace is in, this operation can take as little as 30 seconds and 
  #     upwards to 5 minutes, but this is a one-time operation.""")

  dbutils.fs.cp(source_dir, raw_data_dir, True)
  BI.print(f"""\nThe install of the datasets completed successfully.""")

# COMMAND ----------

padding = "{padding}"
border_color = "#CDCDCD"
font = "font-size:16px;"
weight = "font-weight:bold;"
align = "vertical-align:top;"
border = f"border-bottom:1px solid {border_color};"

def html_intro():
  return """<html><body><table style="width:100%">
            <p style="font-size:16px">The following variables and functions have been defined for you.<br/>Please refer to them in the following instructions.</p>"""

def html_header():
  return f"""<tr><th style="{border} padding: 0 1em 0 0; text-align:left">Variable/Function</th>
                 <th style="{border} padding: 0 1em 0 0; text-align:left">Description</th></tr>"""

def html_row_var(name, value, description):
  return f"""<tr><td style="{font} {weight} {padding} {align} white-space:nowrap; color:green;">{name}</td>
                 <td style="{font} {weight} {padding} {align} white-space:nowrap; color:blue;">{value}</td>
             </tr><tr><td style="{border} {font} {padding}">&nbsp;</td>
                 <td style="{border} {font} {padding} {align}">{description}</td></tr>"""

def html_row_fun(name, description):
  return f"""<tr><td style="{border}; {font} {padding} {align} {weight} white-space:nowrap; color:green;">{name}</td>
                 <td style="{border}; {font} {padding} {align}">{description}</td></td></tr>"""

# COMMAND ----------

html = html_intro()
html += html_header()

html += html_row_fun("install_datasets()", "A utility function for installing datasets into the current workspace.")

html += "</table></body></html>"

displayHTML(html)
