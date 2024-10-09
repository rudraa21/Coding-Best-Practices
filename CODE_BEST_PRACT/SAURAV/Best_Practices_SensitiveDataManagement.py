# Databricks notebook source
# MAGIC %md
# MAGIC ### 🔍 Notebook Overview
# MAGIC This notebook demonstrates best practices for managing sensitive data in Databricks. We avoid hardcoding credentials, utilize Databricks Secrets or Azure Key Vault for storing sensitive information, and ensure that no sensitive data is exposed in logs or output.
# MAGIC
# MAGIC #### 📌 Key Elements:
# MAGIC - **Purpose:** Showcase secure handling of sensitive data.
# MAGIC - **Sensitive Data Storage:** Use Databricks Secrets/Azure Key Vault.
# MAGIC - **Avoid Hardcoding:** Retrieve credentials and sensitive information securely.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📅 Change Log
# MAGIC Tracking changes to ensure that updates related to secure data handling are recorded.
# MAGIC
# MAGIC | Date       | Author          | Description                              | Version |
# MAGIC |------------|-----------------|------------------------------------------|---------|
# MAGIC | 2024-10-02 | Saurav Chaudhary | Added secure data handling using Databricks Secrets | v1.0    |
# MAGIC

# COMMAND ----------

# Accessing sensitive credentials securely from Databricks Secrets
db_username = dbutils.secrets.get(scope="my_scope", key="db_username")
db_password = dbutils.secrets.get(scope="my_scope", key="db_password")


# COMMAND ----------

Display name: file_notification
Application (client) ID: 01494e40-236d-4bcc-bd7b-003858bf4406 celebal_client_id
Object ID:  6e3c20e1-190a-4c06-b1ed-c64fd5493129
Directory (tenant) ID: e4e34038-ea1f-4882-b6e8-ccd776459ca0
 
Value: D328Q~W.OSo6XJqRODYmm29EOm2hMLaObGqKcbkb  celebal_secret_value
Secret ID: 7db4b6ad-bf74-4d36-88d3-e9e5fd1e48c4  

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛡 Avoiding Hardcoded Credentials
# MAGIC Best practices suggest never hardcoding sensitive information like API keys, database credentials, or tokens directly in the notebook code. Instead, sensitive information should be accessed dynamically from secure storage.
# MAGIC

# COMMAND ----------

jdbc_url = "jdbc:sqlserver://<db-server>.database.windows.net:1433;database=<db-name>"
db_properties = {
    "user": dbutils.secrets.get(scope="my_scope", key="db_username"),
    "password": dbutils.secrets.get(scope="my_scope", key="db_password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Securely accessing the database without exposing credentials
df = spark.read.jdbc(url=jdbc_url, table="source_table", properties=db_properties)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨 Secure Logging: Avoid Logging Sensitive Data
# MAGIC Ensure that no sensitive information, such as usernames, passwords, or API tokens, is logged or exposed in notebook outputs or logs.
# MAGIC

# COMMAND ----------

def log_info(info_message):
    """
    A secure logging function that avoids logging sensitive information.
    
    :param info_message: The informational message to log
    """
    if "password" not in info_message.lower():  # Avoid logging sensitive data
        print(f"INFO: {info_message}")
    else:
        print("INFO: Sensitive information is being handled securely.")
        
log_info(f"Successfully connected to the database.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Demo: Secure Data Handling Example
# MAGIC In this example, we ingest data from a database, transform it, and write it back to a target table securely, while ensuring that no sensitive data is exposed.
# MAGIC

# COMMAND ----------

try:
    # Step 1: Securely Ingest Data from a Database
    jdbc_url = "jdbc:sqlserver://<db-server>.database.windows.net:1433;database=<db-name>"
    db_properties = {
        "user": dbutils.secrets.get(scope="my_scope", key="db_username"),
        "password": dbutils.secrets.get(scope="my_scope", key="db_password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    df = spark.read.jdbc(url=jdbc_url, table="source_table", properties=db_properties)
    log_info("Data ingested from the database securely.")
    
    # Step 2: Apply Transformation
    transformed_df = df.groupBy("column1").agg({"column2": "sum"})
    
    # Step 3: Write Data to Target
    transformed_df.write.format("delta").mode("overwrite").saveAsTable("target_table")
    log_info("Data written to the target table securely.")
    
except Exception as e:
    log_error(f"ETL pipeline failed: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Conclusion
# MAGIC This notebook demonstrates secure data handling practices in Databricks by utilizing Databricks Secrets for managing sensitive information, avoiding hardcoded credentials, and implementing secure logging practices to ensure no sensitive data is exposed.
# MAGIC
