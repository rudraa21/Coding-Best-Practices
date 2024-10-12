# Databricks notebook source
# MAGIC %md
# MAGIC ### 🛠 Mail_Request Notebook: `Mail Utility`
# MAGIC This notebook contains send mail functions to send json payload to logic app http url which will send the mail to business users.
# MAGIC

# COMMAND ----------

# MAGIC %run ./HTML_Display_Utility

# COMMAND ----------

# Email notification function
def send_fail_notification(error_message:str, logic_app_url_key:str, databricks_scope:str)-> None:
    """
    Send Sends an email notification with the given error message.

    Args:
        error_message(str): The error message to send.
        logic_app_url_key(str): The databricks secret key for the logic app url
        databricks_scope(str): The databricks secret scope

    """
    try:
        logger.log_info("Sending mail notification...")
        #create the payload for the mail
        payload = {
            "error_message": error_message,
            "notebook_name": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get(),
            "status": "failed"
        }
        headers = {'Content-Type': 'application/json'}
        #get the logic app url from the databricks secret
        logic_app_url = dbutils.secrets.get(scope = databricks_scope, key = logic_app_url_key)
        #send the request to the Logic App
        response = requests.post(logic_app_url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            logger.log_info("mail notification sent.")
        else:
            logger.log_error("Failed to send notification, status code: " + str(response.status_code))
    except Exception as e:
        logger.log_error(f"Error sending failure notification: {str(e)}")
        error.handle_error()
 


# COMMAND ----------

html = html_intro()
html += html_header()
html += html_row_fun("send_fail_notification()", "send mail functions to send json payload to logic app http url which will send the mail to business users")

html += "</table></body></html>"
displayHTML(html)

