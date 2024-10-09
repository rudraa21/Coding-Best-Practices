# Databricks notebook source
# MAGIC %md
# MAGIC ### 🔍 Notebook Overview
# MAGIC This notebook demonstrates how to design ETL pipelines that meet performance and reliability SLAs. Key considerations include excluding performance-heavy operations (like `VACUUM`, `OPTIMIZE`, and `ZORDER`) from the ETL pipeline, running them during off-peak hours, and setting up monitoring and alerts to maintain performance.
# MAGIC
# MAGIC #### 📌 Key Elements:
# MAGIC - **Purpose:** Build ETL pipelines that ensure performance and reliability.
# MAGIC - **Operations Exclusion:** Ensure operations like `VACUUM`, `OPTIMIZE`, and `ZORDER` are not included in the main ETL workflow.
# MAGIC - **Proactive Monitoring:** Set up alerts and logs to monitor pipeline performance.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📅 Change Log
# MAGIC Tracking changes to ensure updates related to SLA and performance management are properly documented.
# MAGIC
# MAGIC | Date       | Author          | Description                              | Version |
# MAGIC |------------|-----------------|------------------------------------------|---------|
# MAGIC | 2024-10-02 | Saurav Chaudhary | Added performance and reliability SLA best practices | v1.0    |
# MAGIC | 2024-10-03 | Saurav Chaudhary | Included proactive monitoring and scheduling of heavy operations | v1.1    |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛠 Excluding Heavy Operations from ETL Workflows
# MAGIC Certain operations such as `VACUUM`, `OPTIMIZE`, `ZORDER`, and table alterations (schema changes) should be excluded from regular ETL workflows to prevent violations of performance SLAs. These operations are resource-intensive and can impact the performance of ETL jobs.
# MAGIC

# COMMAND ----------

def run_etl_pipeline():
    """
    Core ETL pipeline logic that excludes heavy operations like VACUUM and OPTIMIZE.
    """
    try:
        # Step 1: Ingest Data
        df = ingest_data(source_table, filter_condition)
        
        # Step 2: Apply Transformation
        transformed_df = transform_data(df, group_column, agg_column)
        
        # Step 3: Write Transformed Data to Target
        write_data(transformed_df, target_table)
        
        log_info("ETL Pipeline completed successfully.")
    
    except Exception as e:
        log_error(f"ETL Pipeline failed: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### ⏲ Schedule Heavy Operations for Off-Peak Hours
# MAGIC To avoid performance degradation during critical workloads, operations like `VACUUM`, `OPTIMIZE`, and `ZORDER` should be scheduled outside of peak hours. 
# MAGIC

# COMMAND ----------

def run_maintenance_operations():
    """
    Runs VACUUM and OPTIMIZE during off-peak hours to improve table performance.
    """
    try:
        # Run OPTIMIZE on the target table
        spark.sql(f"OPTIMIZE {target_table} ZORDER BY (column_to_zorder)")
        log_info(f"OPTIMIZE operation on {target_table} completed.")
        
        # Run VACUUM to clean up old versions of the Delta table
        spark.sql(f"VACUUM {target_table} RETAIN 168 HOURS")  # Retain last 7 days
        log_info(f"VACUUM operation on {target_table} completed.")
    
    except Exception as e:
        log_error(f"Maintenance operations failed: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨 Proactive Monitoring and Alerts for ETL Pipelines
# MAGIC Setting up alerts and monitoring is critical to managing performance proactively. This ensures that performance degradation or SLA breaches are detected early.
# MAGIC

# COMMAND ----------

# Set up a simple monitoring check for pipeline performance
def monitor_pipeline_performance():
    """
    Monitor the ETL pipeline's performance and raise alerts if SLAs are breached.
    """
    try:
        job_execution_time = get_job_execution_time()  # Hypothetical function to get job time
        
        # Example: If the job execution time exceeds 2 hours, log an alert
        if job_execution_time > 2 * 60 * 60:
            log_error("ETL Pipeline exceeded execution time SLA.")
            # Send an alert using a monitoring tool or email notification
            dbutils.notebook.exit("SLA Breach: ETL Pipeline exceeded execution time limit.")
        else:
            log_info("ETL Pipeline executed within SLA limits.")
    
    except Exception as e:
        log_error(f"Monitoring failed: {str(e)}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Conclusion
# MAGIC This notebook showcases how to design ETL pipelines that meet performance and reliability SLAs by excluding performance-heavy operations from ETL workflows, scheduling them during off-peak hours, and setting up alerts and monitoring to ensure SLAs are met.
# MAGIC

# COMMAND ----------


