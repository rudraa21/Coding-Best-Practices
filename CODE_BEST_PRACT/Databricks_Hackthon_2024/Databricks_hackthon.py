# Databricks notebook source
# MAGIC %md #  Project Overview & Dataset Install
# MAGIC
# MAGIC The Hackthon project aims to assess rudimentary skills as it relates to the Apache Spark and DataFrame APIs.
# MAGIC
# MAGIC The approach taken here assumes that you are familiar with and have some experience with the following entities:
# MAGIC * **`SparkContext`**
# MAGIC * **`SparkSession`**
# MAGIC * **`DataFrame`**
# MAGIC * **`DataFrameReader`**
# MAGIC * **`DataFrameWriter`**
# MAGIC * The various functions found in the module **`pyspark.sql.functions`**
# MAGIC
# MAGIC Throughout this project, you will be given specific instructions and it is our expectation that you will be able to complete these instructions drawing on your existing knowledge as well as other sources such as the <a href="https://spark.apache.org/docs/latest/api.html" target="_blank">Spark API Documentation</a>.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md ### The Data
# MAGIC The raw data comes in three forms:
# MAGIC
# MAGIC 1. Orders that were processed in 2017, 2018 and 2019.
# MAGIC   * For each year a separate batch (or backup) of that year's orders was produced
# MAGIC   * The format of all three files are similar, but were not produced exactly the same:
# MAGIC     * 2017 is in a fixed-width text file format
# MAGIC     * 2018 is tab-separated text file
# MAGIC     * 2019 is comma-separated text file
# MAGIC   * Each order consists for four main data points:
# MAGIC     0. The order - the highest level aggregate
# MAGIC     0. The line items - the individual products purchased in the order
# MAGIC     0. The sales reps - the person placing the order
# MAGIC     0. The customer - the person who purchased the items and where it was shipped.
# MAGIC   * All three batches are consistent in that there is one record per line item creating a significant amount of duplicated data across orders, reps and customers.
# MAGIC   * All entities are generally referenced by an ID, such as order_id, customer_id, etc.
# MAGIC   
# MAGIC 2. All products to be sold by this company (SKUs) are represented in a single XML file
# MAGIC
# MAGIC 3. In 2020, the company switched systems and now lands a single JSON file in cloud storage for every order received.
# MAGIC   * These orders are simplified versions of the batched data fro 2017-2019 and includes only the order's details, the line items, and the correlating ids
# MAGIC   * The sales reps's data is no longer represented in conjunction with an order

# COMMAND ----------

# MAGIC %md ### The Exercises
# MAGIC
# MAGIC * In **Exercise #1**, (this notebook) we introduce  the installation of our datasets.
# MAGIC
# MAGIC * In **Exercise #2**, we will ingest the batch data for 2017-2019, combine them into a single dataset for future processing.
# MAGIC
# MAGIC * In **Exercise #3**, we will take the unified batch data from **Exercise #2**, clean it, and extract it into three new datasets: Orders, Line Items and Sales Reps. The customer data, for the sake of simplicity, will not be broken out and left with the orders.
# MAGIC
# MAGIC * In **Exercise #4**, we will ingest the XML document containing all the projects, and combine it with the Line Items to create yet another dataset, Product Line Items.
# MAGIC
# MAGIC * In **Exercise #5**, we will begin processing the stream of orders for 2020, appending that stream of data to the existing datasets as necessary.
# MAGIC
# MAGIC * In **Exercise #6**, we will use all of our new datasets to answer a handful of business questions.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Setup Exercise #1</h2>
# MAGIC
# MAGIC To get started, we first need to configure your Registration ID and then run the setup notebook.

# COMMAND ----------

# MAGIC %md ### Setup - Create A Cluster
# MAGIC
# MAGIC #### Databricks Community Edition
# MAGIC
# MAGIC This Capstone project was designed to work with Databricks Runtime Version (DBR) 12.2 LTS and the Databricks Community Edition's (CE) default cluster configuration. 
# MAGIC
# MAGIC When working in CE, start a default cluster, specify **DBR 12.2 LTS**, and then proceede with the next step. 
# MAGIC
# MAGIC #### Other than Community Edition (MSA, AWS or GCP)
# MAGIC
# MAGIC This capstone project was designed to work with a small, single-node cluster when not using CE. When configuring your cluster, please specify the following:
# MAGIC
# MAGIC * DBR: **12.2 LTS** 
# MAGIC * Cluster Mode: **Single Node**
# MAGIC * Node Type: 
# MAGIC   * for Microsoft Azure - **Standard_E4ds_v4**
# MAGIC   * for Amazon Web Services - **i3.xlarge** 
# MAGIC   * for Google Cloud Platform - **n1-highmem-4** 
# MAGIC
# MAGIC Please feel free to use the Community Edition if the recomended node types are not available.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #1 - Install Datasets</h2>
# MAGIC
# MAGIC The datasets for this project are stored in a public object store.
# MAGIC
# MAGIC They need to be downloaded and installed into your Databricks workspace before proceeding with this project.
# MAGIC
# MAGIC But before doing that, we need to configure a cluster appropriate for this project.

# COMMAND ----------

# MAGIC %run ./Dataset_Installer/DatasetInstaller

# COMMAND ----------

# At any time during this project, you can reinstall the source datasets
# by setting reinstall=True. These datasets will not be automtically 
# reinstalled when this notebook is re-ran so as to save you time.
install_datasets(reinstall=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #2 - Batch Ingestion
# MAGIC
# MAGIC In this exercise you will be ingesting three batches of orders, one for 2017, 2018 and 2019.
# MAGIC
# MAGIC As each batch is ingested, we are going to append it to a new Delta table, unifying all the datasets into one single dataset.
# MAGIC
# MAGIC Each year, different individuals and different standards were used resulting in datasets that vary slightly:
# MAGIC * In 2017 the backup was written as fixed-width text files
# MAGIC * In 2018 the backup was written a tab-separated text files
# MAGIC * In 2019 the backup was written as a "standard" comma-separted text files but the format of the column names was changed
# MAGIC
# MAGIC Our only goal here is to unify all the datasets while tracking the source of each record (ingested file name and ingested timestamp) should additional problems arise.
# MAGIC
# MAGIC Because we are only concerned with ingestion at this stage, the majority of the columns will be ingested as simple strings and in future exercises we will address this issue (and others) with various transformations.
# MAGIC
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 2.A - Ingest Fixed-Width File
# MAGIC * Exercise 2.B - Ingest Tab-Separated File
# MAGIC * Exercise 2.C - Ingest Comma-Separated File

# COMMAND ----------

# DBTITLE 1,Path for data set
print( batch_2017_path, """The path to the 2017 batch of orders""")
print( batch_2018_path, """The path to the 2018 batch of orders""")
print( batch_2019_path, """The path to the 2019 batch of orders""")
print( batch_target_path, """The location of the new, unified, raw, batch of orders & sales reps""")

# COMMAND ----------

files = dbutils.fs.ls(f"{working_dir}/raw/orders/batch") # List all the files
display(files)    # Display the list of files

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.A - Ingest Fixed-Width File</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2017_path`**, and **`dbutils.fs.head`** to investigate the 2017 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the text file identified by **`batch_2017_path`** - this should provide one record per line, with a single column named **`value`**
# MAGIC 3. Using the information in **`fixed_width_column_defs`** (or the dictionary itself) use the **`value`** column to extract each new column of the appropriate length.<br/>
# MAGIC   * The dictionary's key is the column name
# MAGIC   * The first element in the dictionary's value is the starting position of that column's data
# MAGIC   * The second element in the dictionary's value is the length of that column's data
# MAGIC 4. Once you are done with the **`value`** column, remove it.
# MAGIC 5. For each new column created in step #3, remove any leading whitespace
# MAGIC   * The introduction of \[leading\] white space should be expected when extracting fixed-width values out of the **`value`** column.
# MAGIC 6. For each new column created in step #3, replace all empty strings with **`null`**.
# MAGIC   * After trimming white space, any column for which a value was not specified in the original dataset should result in an empty string.
# MAGIC 7. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from.
# MAGIC   * This should not be hard coded.
# MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
# MAGIC 8. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame.
# MAGIC   * This should not be hard coded.
# MAGIC   * For the proper function, see the <a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">pyspark.sql.functions</a> module
# MAGIC 9. Write the corresponding **`DataFrame`** in the "delta" format to the location specified by **`batch_target_path`**
# MAGIC
# MAGIC **Special Notes:**
# MAGIC * It is possible to use the dictionary **`fixed_width_column_defs`** and programatically extract <br/>
# MAGIC   each column but, it is also perfectly OK to hard code this step and extract one column at a time.
# MAGIC * The **`SparkSession`** is already provided to you as an instance of **`spark`**.
# MAGIC * The classes/methods that you will need for this exercise include:
# MAGIC   * **`pyspark.sql.DataFrameReader`** to ingest data
# MAGIC   * **`pyspark.sql.DataFrameWriter`** to write data
# MAGIC   * **`pyspark.sql.Column`** to transform data
# MAGIC   * Various functions from the **`pyspark.sql.functions`** module
# MAGIC   * Various transformations and actions from **`pyspark.sql.DataFrame`**
# MAGIC * The following methods can be used to investigate and manipulate the Databricks File System (DBFS)
# MAGIC   * **`dbutils.fs.ls(..)`** for listing files
# MAGIC   * **`dbutils.fs.rm(..)`** for removing files
# MAGIC   * **`dbutils.fs.head(..)`** to view the first N bytes of a file
# MAGIC
# MAGIC **Additional Requirements:**
# MAGIC * The unified batch dataset must be written to disk in the "delta" format
# MAGIC * The schema for the unified batch dataset must be:
# MAGIC   * **`submitted_at`**:**`string`**
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`customer_id`**:**`string`**
# MAGIC   * **`sales_rep_id`**:**`string`**
# MAGIC   * **`sales_rep_ssn`**:**`string`**
# MAGIC   * **`sales_rep_first_name`**:**`string`**
# MAGIC   * **`sales_rep_last_name`**:**`string`**
# MAGIC   * **`sales_rep_address`**:**`string`**
# MAGIC   * **`sales_rep_city`**:**`string`**
# MAGIC   * **`sales_rep_state`**:**`string`**
# MAGIC   * **`sales_rep_zip`**:**`string`**
# MAGIC   * **`shipping_address_attention`**:**`string`**
# MAGIC   * **`shipping_address_address`**:**`string`**
# MAGIC   * **`shipping_address_city`**:**`string`**
# MAGIC   * **`shipping_address_state`**:**`string`**
# MAGIC   * **`shipping_address_zip`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`string`**
# MAGIC   * **`product_sold_price`**:**`string`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### Fixed-Width Meta Data 
# MAGIC
# MAGIC The following dictionary is provided for reference and/or implementation<br/>
# MAGIC (depending on which strategy you choose to employ).
# MAGIC
# MAGIC Run the following cell to instantiate it.

# COMMAND ----------

fixed_width_column_defs = {
  "submitted_at": (1, 15),
  "order_id": (16, 40),
  "customer_id": (56, 40),
  "sales_rep_id": (96, 40),
  "sales_rep_ssn": (136, 15),
  "sales_rep_first_name": (151, 15),
  "sales_rep_last_name": (166, 15),
  "sales_rep_address": (181, 40),
  "sales_rep_city": (221, 20),
  "sales_rep_state": (241, 2),
  "sales_rep_zip": (243, 5),
  "shipping_address_attention": (248, 30),
  "shipping_address_address": (278, 40),
  "shipping_address_city": (318, 20),
  "shipping_address_state": (338, 2),
  "shipping_address_zip": (340, 5),
  "product_id": (345, 40),
  "product_quantity": (385, 5),
  "product_sold_price": (390, 20)
}

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.A
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.B - Ingest Tab-Separted File</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2018_path`**, and **`dbutils.fs.head`** to investigate the 2018 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the tab-separated file identified by **`batch_2018_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created datasets specified by **`batch_target_path`**
# MAGIC
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.b
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #2.C - Ingest Comma-Separted File</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC 1. Use the variable **`batch_2019_path`**, and **`dbutils.fs.head`** to investigate the 2019 batch file, if needed.
# MAGIC 2. Configure a **`DataFrameReader`** to ingest the comma-separated file identified by **`batch_2019_path`**
# MAGIC 3. Add a new column, **`ingest_file_name`**, which is the name of the file from which the data was read from - note this should not be hard coded.
# MAGIC 4. Add a new column, **`ingested_at`**, which is a timestamp of when the data was ingested as a DataFrame - note this should not be hard coded.
# MAGIC 5. **Append** the corresponding **`DataFrame`** to the previously created dataset specified by **`batch_target_path`**<br/>
# MAGIC    Note: The column names in this dataset must be updated to conform to the schema defined for Exercise #2.A - there are several strategies for this:
# MAGIC    * Provide a schema that alters the names upon ingestion
# MAGIC    * Manually rename one column at a time
# MAGIC    * Use **`fixed_width_column_defs`** programaticly rename one column at a time
# MAGIC    * Use transformations found in the **`DataFrame`** class to rename all columns in one operation
# MAGIC
# MAGIC **Additional Requirements**
# MAGIC * Any **"null"** strings in the CSV file should be replaced with the SQL value **null**<br/>

# COMMAND ----------

# MAGIC %md ### Implement Exercise #2.c
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #3 - Create Fact & Dim Tables
# MAGIC
# MAGIC Now that the three years of orders are combined into a single dataset, we can begin the processes of transforming the data.
# MAGIC
# MAGIC In the one record, there are actually four sub-datasets:
# MAGIC * The order itself which is the aggregator of the other three datasets.
# MAGIC * The line items of each order which includes the price and quantity of each specific item.
# MAGIC * The sales rep placing the order.
# MAGIC * The customer placing the order - for the sake of simplicity, we will **not** break this dataset out and leave it as part of the order.
# MAGIC
# MAGIC What we want to do next, is to extract all that data into their respective datasets (except the customer data). 
# MAGIC
# MAGIC In other words, we want to normalize the data, in this case, to reduce data duplication.
# MAGIC
# MAGIC This exercise is broken up into 5 steps:
# MAGIC * Exercise 3.A - Create & Use Database
# MAGIC * Exercise 3.B - Load & Cache Batch Orders
# MAGIC * Exercise 3.C - Extract Sales Reps
# MAGIC * Exercise 3.D - Extract Orders
# MAGIC * Exercise 3.E - Extract Line Items

# COMMAND ----------

# DBTITLE 1,Variables in Exercise 3
print(user_db,	" The name of the database you will use for this project.")
print(batch_source_path, " The location of the combined, raw, batch of orders.")
print(orders_table," The name of the orders table.")
print(line_items_table," The name of the line items table.")
print(sales_reps_table, " The name of the sales reps table.")
print(batch_temp_view,	" The name of the temp view used in this exercise")

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.A - Create &amp; Use Database</h2>
# MAGIC
# MAGIC By using a specific database, we can avoid contention to commonly named tables that may be in use by other users of the workspace.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Create the database identified by the variable **`user_db`**
# MAGIC * Use the database identified by the variable **`user_db`** so that any tables created in this notebook are **NOT** added to the **`default`** database
# MAGIC
# MAGIC **Special Notes**
# MAGIC * Do not hard-code the database name - in some scenarios this will result in validation errors.
# MAGIC * For assistence with the SQL command to create a database, see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-database.html" target="_blank">CREATE DATABASE</a> on the Databricks docs website.
# MAGIC * For assistence with the SQL command to use a database, see <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-usedb.html" target="_blank">USE DATABASE</a> on the Databricks docs website.

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.A
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.B - Load &amp; Cache Batch Orders</h2>
# MAGIC
# MAGIC Next, we need to load the batch orders from the previous exercise and then cache them in preparation to transform the data later in this exercise.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the delta dataset we created in the previous exercise, identified by the variable **`batch_source_path`**.
# MAGIC * Using that same dataset, create a temporary view identified by the variable **`batch_temp_view`**.
# MAGIC * Cache the temporary view.

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.B
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.C - Extract Sales Reps</h2>
# MAGIC
# MAGIC Our batched orders from Exercise #2 contains thousands of orders and with every order, is the name, SSN, address and other information on the sales rep making the order.
# MAGIC
# MAGIC We can use this data to create a table of just our sales reps.
# MAGIC
# MAGIC If you consider that we have only ~100 sales reps, but thousands of orders, we are going to have a lot of duplicate data in this space.
# MAGIC
# MAGIC Also unique to this set of data, is the fact that social security numbers were not always sanitized meaning sometime they were formatted with hyphens and in other cases they were not - this is something we will have to address here.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * The SSN numbers have errors in them that we want to track - add the **`boolean`** column **`_error_ssn_format`** - for any case where **`sales_rep_ssn`** has a hypen in it, set this value to **`true`** otherwise **`false`**
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`sales_rep_ssn`** should be represented as a **`Long`** (Note: You will have to first clean the column by removing extreneous hyphens in some records)
# MAGIC   * The column **`sales_rep_zip`** should be represented as an **`Integer`**
# MAGIC * Remove the columns not directly related to the sales-rep record:
# MAGIC   * Unrelated ID columns: **`submitted_at`**, **`order_id`**, **`customer_id`**
# MAGIC   * Shipping address columns: **`shipping_address_attention`**, **`shipping_address_address`**, **`shipping_address_city`**, **`shipping_address_state`**, **`shipping_address_zip`**
# MAGIC   * Product columns: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * Because there is one record per product ordered (many products per order), not to mention one sales rep placing many orders (many orders per sales rep), there will be duplicate records for our sales reps. Remove all duplicate records.
# MAGIC * Load the dataset to the managed delta table **`sales_rep_scd`** (identified by the variable **`sales_reps_table`**)
# MAGIC
# MAGIC **Additional Requirements:**<br/>
# MAGIC The schema for the **`sales_rep_scd`** table must be:
# MAGIC * **`sales_rep_id`**:**`string`**
# MAGIC * **`sales_rep_ssn`**:**`long`**
# MAGIC * **`sales_rep_first_name`**:**`string`**
# MAGIC * **`sales_rep_last_name`**:**`string`**
# MAGIC * **`sales_rep_address`**:**`string`**
# MAGIC * **`sales_rep_city`**:**`string`**
# MAGIC * **`sales_rep_state`**:**`string`**
# MAGIC * **`sales_rep_zip`**:**`integer`**
# MAGIC * **`ingest_file_name`**:**`string`**
# MAGIC * **`ingested_at`**:**`timestamp`**
# MAGIC * **`_error_ssn_format`**:**`boolean`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.C
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.D - Extract Orders</h2>
# MAGIC
# MAGIC Our batched orders from Exercise 02 contains one line per product meaning there are multiple records per order.
# MAGIC
# MAGIC The goal of this step is to extract just the order details (excluding the sales rep and line items)
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`submitted_at`** is a "unix epoch" (number of seconds since 1970-01-01 00:00:00 UTC) and should be represented as a **`Timestamp`**
# MAGIC   * The column **`shipping_address_zip`** should be represented as an **`Integer`**
# MAGIC * Remove the columns not directly related to the order record:
# MAGIC   * Sales reps columns: **`sales_rep_ssn`**, **`sales_rep_first_name`**, **`sales_rep_last_name`**, **`sales_rep_address`**, **`sales_rep_city`**, **`sales_rep_state`**, **`sales_rep_zip`**
# MAGIC   * Product columns: **`product_id`**, **`product_quantity`**, **`product_sold_price`**
# MAGIC * Because there is one record per product ordered (many products per order), there will be duplicate records for each order. Remove all duplicate records.
# MAGIC * Add the column **`submitted_yyyy_mm`** which is a **`string`** derived from **`submitted_at`** and is formatted as "**yyyy-MM**".
# MAGIC * Load the dataset to the managed delta table **`orders`** (identified by the variable **`orders_table`**)
# MAGIC   * In thise case, the data must also be partitioned by **`submitted_yyyy_mm`**
# MAGIC
# MAGIC **Additional Requirements:**
# MAGIC * The schema for the **`orders`** table must be:
# MAGIC   * **`submitted_at:timestamp`**
# MAGIC   * **`submitted_yyyy_mm`** using the format "**yyyy-MM**"
# MAGIC   * **`order_id:string`**
# MAGIC   * **`customer_id:string`**
# MAGIC   * **`sales_rep_id:string`**
# MAGIC   * **`shipping_address_attention:string`**
# MAGIC   * **`shipping_address_address:string`**
# MAGIC   * **`shipping_address_city:string`**
# MAGIC   * **`shipping_address_state:string`**
# MAGIC   * **`shipping_address_zip:integer`**
# MAGIC   * **`ingest_file_name:string`**
# MAGIC   * **`ingested_at:timestamp`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.D
# MAGIC
# MAGIC Implement your solution in the following cell:
# MAGIC

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #3.E - Extract Line Items</h2>
# MAGIC
# MAGIC Now that we have extracted sales reps and orders, we next want to extract the specific line items of each order.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the table **`batched_orders`** (identified by the variable **`batch_temp_view`**)
# MAGIC * Retain the following columns (see schema below)
# MAGIC   * The correlating ID columns: **`order_id`** and **`product_id`**
# MAGIC   * The two product-specific columns: **`product_quantity`** and **`product_sold_price`**
# MAGIC   * The two ingest columns: **`ingest_file_name`** and **`ingested_at`**
# MAGIC * Convert various columns from their string representation to the specified type:
# MAGIC   * The column **`product_quantity`** should be represented as an **`Integer`**
# MAGIC   * The column **`product_sold_price`** should be represented as an **`Decimal`** with two decimal places as in **`decimal(10,2)`**
# MAGIC * Load the dataset to the managed delta table **`line_items`** (identified by the variable **`line_items_table`**)
# MAGIC
# MAGIC **Additional Requirements:**
# MAGIC * The schema for the **`line_items`** table must be:
# MAGIC   * **`order_id`**:**`string`**
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`product_quantity`**:**`integer`**
# MAGIC   * **`product_sold_price`**:**`decimal(10,2)`**
# MAGIC   * **`ingest_file_name`**:**`string`**
# MAGIC   * **`ingested_at`**:**`timestamp`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #3.E
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #4 - XML Ingestion, Products Table
# MAGIC
# MAGIC The products being sold by our sales reps are itemized in an XML document which we will need to load.
# MAGIC
# MAGIC Unlike CSV, JSON, Parquet, & Delta, support for XML is not included with the default distribution of Apache Spark.
# MAGIC
# MAGIC Before we can load the XML document, we need additional support for a **`DataFrameReader`** that can processes XML files.
# MAGIC
# MAGIC Once the **spark-xml** library is installed to our cluster, we can load our XML document and proceede with our other transformations.
# MAGIC
# MAGIC This exercise is broken up into 4 steps:
# MAGIC * Exercise 4.A - Install Library
# MAGIC * Exercise 4.B - Load Products

# COMMAND ----------

# DBTITLE 1,Variables in exercise 4
print(user_db ," The name of the database you will use for this project.")
print(products_table ," The name of the products table.")
print(products_xml_path, "The location of the product's XML file")

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.A - Install Library</h2>
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Register the **spark-xml** library - edit your cluster configuration and then from the **Libraries** tab, install the following library:
# MAGIC   * Type: **Maven**
# MAGIC   * Coordinates: **com.databricks:spark-xml_2.12:0.10.0**
# MAGIC
# MAGIC If you are unfamiliar with this processes, more information can be found in the <a href="https://docs.databricks.com/libraries/cluster-libraries.html" target="_blank">Cluster libraries documentation</a>.
# MAGIC
# MAGIC Once the library is installed, run the following reality check to confirm proper installation.<br/>
# MAGIC Note: You may need to restart the cluster after installing the library for you changes to take effect.

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #4.B - Load Products</h2>
# MAGIC
# MAGIC With the **spark-xml** library installed, ingesting an XML document is identical to ingesting any other dataset - other than specific, provided, options.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Load the XML document using the following paramters:
# MAGIC   * Format: **xml**
# MAGIC   * Options:
# MAGIC     * **`rootTag`** = **`products`** - identifies the root tag in the XML document, in our case this is "products"
# MAGIC     * **`rowTag`** = **`product`** - identifies the tag of each row under the root tag, in our case this is "product"
# MAGIC     * **`inferSchema`** = **`True`** - The file is small, and a one-shot operation - infering the schema will save us some time
# MAGIC   * File Path: specified by the variable **`products_xml_path`**
# MAGIC   
# MAGIC * Update the schema to conform to the following specification:
# MAGIC   * **`product_id`**:**`string`**
# MAGIC   * **`color`**:**`string`**
# MAGIC   * **`model_name`**:**`string`**
# MAGIC   * **`model_number`**:**`string`**
# MAGIC   * **`base_price`**:**`double`**
# MAGIC   * **`color_adj`**:**`double`**
# MAGIC   * **`size_adj`**:**`double`**
# MAGIC   * **`price`**:**`double`**
# MAGIC   * **`size`**:**`string`**
# MAGIC
# MAGIC * Exclude any records for which a **`price`** was not included - these represent products that are not yet available for sale.
# MAGIC * Load the dataset to the managed delta table **`products`** (identified by the variable **`products_table`**)

# COMMAND ----------

# MAGIC %md ### Implement Exercise #4.B
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #5 - Streaming Orders
# MAGIC
# MAGIC With our four historical datasets properly loaded, we can now begin to process the "current" orders.
# MAGIC
# MAGIC In this case, the new "system" is landing one JSON file per order into cloud storage.
# MAGIC
# MAGIC We can process these JSON files as a stream of orders under the assumption that new orders are continually added to this dataset.
# MAGIC
# MAGIC We have reduced the "stream" of orders to just the first few hours of 2020 and will be throttling that stream to only one file per iteration.
# MAGIC
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 5.A- Stream-Append Orders
# MAGIC * Exercise 5.B - Stream-Append Line Items
# MAGIC
# MAGIC ## Some Friendly Advice...
# MAGIC
# MAGIC Each record is a JSON object with roughly the following structure:
# MAGIC
# MAGIC * **`customerID`**
# MAGIC * **`orderId`**
# MAGIC * **`products`**
# MAGIC   * array
# MAGIC     * **`productId`**
# MAGIC     * **`quantity`**
# MAGIC     * **`soldPrice`**
# MAGIC * **`salesRepId`**
# MAGIC * **`shippingAddress`**
# MAGIC   * **`address`**
# MAGIC   * **`attention`**
# MAGIC   * **`city`**
# MAGIC   * **`state`**
# MAGIC   * **`zip`**
# MAGIC * **`submittedAt`**
# MAGIC
# MAGIC As you ingest this data, it will need to be transformed to match the existing **`orders`** table's schema and the **`line_items`** table's schema.
# MAGIC
# MAGIC Furthermore, creating a stream from JSON files will first require you to specify the schema - you can "cheat" and infer that schema from some of the JSON files before starting the stream.

# COMMAND ----------

# DBTITLE 1,Variables in exercise 5
print(user_db	," The name of the database you will use for this project.")
print(orders_table ," The name of the orders table.")
print(products_table	," The name of the products table.")
print(line_items_table ," The name of the line items table.")
print(stream_path	," The path to the stream directory of JSON orders.")
print(orders_checkpoint_path	," The location of the checkpoint for streamed orders.")
print(line_items_checkpoint_path ," The location of the checkpoint for streamed line-items.")

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.A - Stream-Append Orders</h2>
# MAGIC
# MAGIC Every JSON file ingested by our stream representes one order and the enumerated list of products purchased in that order.
# MAGIC
# MAGIC Our goal is simple, ingest the data, transform it as required by the **`orders`** table's schema, and append these new records to our existing table.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC
# MAGIC * Ingest the stream of JSON files:
# MAGIC   * Start a stream from the path identified by **`stream_path`**.
# MAGIC   * Using the **`maxFilesPerTrigger`** option, throttle the stream to process only one file per iteration.
# MAGIC   * Add the ingest meta data (same as with our other datasets):
# MAGIC     * **`ingested_at`**:**`timestamp`**
# MAGIC     * **`ingest_file_name`**:**`string`**
# MAGIC   * Properly parse the **`submitted_at`**  as a valid **`timestamp`**
# MAGIC   * Add the column **`submitted_yyyy_mm`** usinge the format "**yyyy-MM**"
# MAGIC   * Make any other changes required to the column names and data types so that they conform to the **`orders`** table's schema
# MAGIC
# MAGIC * Write the stream to a Delta **table**.:
# MAGIC   * The table's format should be "**delta**"
# MAGIC   * Partition the data by the column **`submitted_yyyy_mm`**
# MAGIC   * Records must be appended to the table identified by the variable **`orders_table`**
# MAGIC   * The query must be named the same as the table, identified by the variable **`orders_table`**
# MAGIC   * The query must use the checkpoint location identified by the variable **`orders_checkpoint_path`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #5.A
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #5.B - Stream-Append Line Items</h2>
# MAGIC
# MAGIC The same JSON file we processed in the previous stream also contains the line items which we now need to extract and append to the existing **`line_items`** table.
# MAGIC
# MAGIC Just like before, our goal is simple, ingest the data, transform it as required by the **`line_items`** table's schema, and append these new records to our existing table.
# MAGIC
# MAGIC Note: we are processing the same stream twice - there are other patterns to do this more efficiently, but for this exercise, we want to keep the design simple.<br/>
# MAGIC The good news here is that you can copy most of the code from the previous step to get you started here.
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC
# MAGIC * Ingest the stream of JSON files:
# MAGIC   * Start a stream from the path identified by **`stream_path`**.
# MAGIC   * Using the **`maxFilesPerTrigger`** option, throttle the stream to process only one file per iteration.
# MAGIC   * Add the ingest meta data (same as with our other datasets):
# MAGIC     * **`ingested_at`**:**`timestamp`**
# MAGIC     * **`ingest_file_name`**:**`string`**
# MAGIC   * Make any other changes required to the column names and data types so that they conform to the **`line_items`** table's schema
# MAGIC     * The most significant transformation will be to the **`products`** column.
# MAGIC     * Add the ingest meta data (**`ingest_file_name`** and **`ingested_at`**).
# MAGIC     * Convert data types as required by the **`line_items`** table's schema.
# MAGIC
# MAGIC * Write the stream to a Delta sink:
# MAGIC   * The sink's format should be "**delta**"
# MAGIC   * Records must be appended to the table identified by the variable **`line_items_table`**
# MAGIC   * The query must be named the same as the table, identified by the variable **`line_items_table`**
# MAGIC   * The query must use the checkpoint location identified by the variable **`line_items_checkpoint_path`**

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exercise #6 - Business Questions
# MAGIC
# MAGIC In our last exercise, we are going to execute various joins across our four tables (**`orders`**, **`line_items`**, **`sales_reps`** and **`products`**) to answer basic business questions
# MAGIC
# MAGIC This exercise is broken up into 3 steps:
# MAGIC * Exercise 6.A - Question #1
# MAGIC * Exercise 6.B - Question #2
# MAGIC * Exercise 6.C - Question #3

# COMMAND ----------

# DBTITLE 1,Variable in exercise 6
print(user_db	," The name of the database you will use for this project.")
print(orders_table ," The name of the orders table.")
print(products_table ," The name of the products table.")
print(line_items_table ," The name of the line items table.")
print(sales_reps_table ," The name of the sales reps table.")
print(question_1_results_table ," The name of the temporary view for the results to question.")
print(question_2_results_table ," The name of the temporary view for the results to question.")
print(question_3_results_table ," The name of the temporary view for the results to question.")

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.A - Question #1</h2>
# MAGIC ## How many orders were shipped to each state?
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Execute the query to get the result for above KPI
# MAGIC * Save the results to the temporary view **`question_1_results`**, identified by the variable **`question_1_results_table`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.A
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.B - Question #2</h2>
# MAGIC ## What is the average, minimum and maximum sale price for green products sold to North Carolina where the Sales Rep submitted an invalid Social Security Number (SSN)?
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Execute tghe query to get the result for above mentioned KPI
# MAGIC * Save the results to the temporary view **`question_2_results`**, identified by the variable **`question_2_results_table`**
# MAGIC * The temporary view should have the following three columns: **`avg(product_sold_price)`**, **`min(product_sold_price)`**, **`max(product_sold_price)`**
# MAGIC * Collect the results to the driver
# MAGIC * Assign to the following local variables, the average, minimum, and maximum values - these variables will be passed to the reality check for validation.
# MAGIC  * **`ex_avg`** - the local variable holding the average value
# MAGIC  * **`ex_min`** - the local variable holding the minimum value
# MAGIC  * **`ex_max`** - the local variable holding the maximum value

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.B
# MAGIC
# MAGIC Implement your solution in the following cell:

# COMMAND ----------

# MAGIC %md <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Exercise #6.C - Question #3</h2>
# MAGIC ## What is the first and last name of the top earning sales rep based on net sales?
# MAGIC
# MAGIC For this scenario...
# MAGIC * The top earning sales rep will be identified as the individual producing the largest profit.
# MAGIC * Profit is defined as the difference between **`product_sold_price`** and **`price`** which is then<br/>
# MAGIC   multiplied by **`product_quantity`** as seen in **(product_sold_price - price) * product_quantity**
# MAGIC
# MAGIC **In this step you will need to:**
# MAGIC * Execute a query to get the result for above KPI
# MAGIC * Save the results to the temporary view **`question_3_results`**, identified by the variable **`question_3_results_table`**
# MAGIC * The temporary view should have the following three columns: 
# MAGIC   * **`sales_rep_first_name`** - the first column by which to aggregate by
# MAGIC   * **`sales_rep_last_name`** - the second column by which to aggregate by
# MAGIC   * **`sum(total_profit)`** - the summation of the column **`total_profit`**

# COMMAND ----------

# MAGIC %md ### Implement Exercise #6.C
# MAGIC
# MAGIC Implement your solution in the following cell:
