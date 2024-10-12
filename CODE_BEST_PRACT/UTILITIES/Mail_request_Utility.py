# Databricks notebook source
# Logic App URL (from the HTTP trigger of your Logic App)
logic_app_url = 'https://prod-26.centralindia.logic.azure.com:443/workflows/cd187a16ecbc4dc28ea2fca07af14e5d/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=Un4yV5sQbjGSRfkt1vc5aCr0BWBmIbN04DmTUoGziTE'
 
# Email notification function
def send_mail_notification(error_message):
    try:
        logger.log_info("Sending mail notification...")
        #create the payload for the mail
        payload = {
            "error_message": error_message,
            "notebook_name": dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        }
        headers = {'Content-Type': 'application/json'}
        #send the request to the Logic App
        response = requests.post(logic_app_url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            logger.log_info("mail notification sent.")
        else:
            logger.log_error("Failed to send notification, status code:", response.status_code)
    except Exception as e:
        logger.log_error(f"Error sending failure notification: {str(e)}")
        error.handle_error(e)
 

