# Databricks notebook source
# MAGIC %md
# MAGIC ### 🔍 Notebook Overview
# MAGIC This notebook demonstrates the importance of modularization and the DRY (Don't Repeat Yourself) principle by implementing reusable, parameterized functions for common tasks like data ingestion, transformation, and writing to target tables.
# MAGIC
# MAGIC #### 📌 Key Elements:
# MAGIC - **Purpose:** Implementing modular, reusable code to avoid redundancy.
# MAGIC - **Source:** A dummy source table `source_table`.
# MAGIC - **Target:** A dummy target table `target_table`.
# MAGIC - **Parameters:** Dynamic job parameters to avoid hardcoding.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📅 Change Log
# MAGIC Tracking changes ensures that updates are recorded, and issues can be traced back to specific versions of the notebook.
# MAGIC
# MAGIC | Date       | Author          | Description                              | Version |
# MAGIC |------------|-----------------|------------------------------------------|---------|
# MAGIC | 2024-10-02 | Saurav Chaudhary | Implemented modular functions and fault-tolerant ETL | v1.0    |
# MAGIC

# COMMAND ----------

def ingest_data(table_name, filter_condition):
    """
    Ingest data from the given source table and apply the necessary filter.
    
    :param table_name: Name of the table to read data from
    :param filter_condition: SQL-based condition for filtering the data
    :return: Spark DataFrame
    """
    try:
        df = spark.table(table_name).filter(filter_condition)
        return df
    except Exception as e:
        log_error(f"Error ingesting data from {table_name}: {str(e)}")
        raise


# COMMAND ----------

def transform_data(df, group_column, agg_column):
    """
    Apply transformation logic to the DataFrame, including aggregation.
    
    :param df: Input DataFrame
    :param group_column: Column to group by
    :param agg_column: Column for aggregation
    :return: Transformed DataFrame
    """
    try:
        transformed_df = df.groupBy(group_column).agg({agg_column: "sum"})
        return transformed_df
    except Exception as e:
        log_error(f"Error transforming data: {str(e)}")
        raise


# COMMAND ----------

def write_data(df, target_table):
    """
    Write the transformed data to the target Delta table.
    
    :param df: DataFrame to be written
    :param target_table: Name of the target table
    """
    try:
        df.write.format("delta").mode("overwrite").saveAsTable(target_table)
    except Exception as e:
        log_error(f"Error writing data to {target_table}: {str(e)}")
        raise


# COMMAND ----------

def log_error(error_message):
    """
    Log errors encountered during execution.
    
    :param error_message: The error message to log
    """
    print(f"ERROR: {error_message}")
    # You can also log this to a file or external logging system


# COMMAND ----------

# Defining parameters
source_table = dbutils.widgets.get("source_table")
target_table = dbutils.widgets.get("target_table")
filter_condition = dbutils.widgets.get("filter_condition")
group_column = dbutils.widgets.get("group_column")
agg_column = dbutils.widgets.get("agg_column")


# COMMAND ----------

try:
    # Step 1: Ingest Data
    df = ingest_data(source_table, filter_condition)
    display(df)
    
    # Step 2: Apply Transformation
    transformed_df = transform_data(df, group_column, agg_column)
    display(transformed_df)
    
    # Step 3: Write Transformed Data to Target
    write_data(transformed_df, target_table)
    
except Exception as e:
    log_error(f"ETL Pipeline failed: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Conclusion
# MAGIC This notebook demonstrates how to implement modularization and the DRY principle by using reusable, parameterized functions. It also showcases fault-tolerant design by including robust error handling and logging. By doing this, we can create cleaner, more maintainable, and scalable Databricks notebooks.
# MAGIC

# COMMAND ----------


