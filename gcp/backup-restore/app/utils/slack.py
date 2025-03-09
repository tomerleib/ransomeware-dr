import os
import json
import requests

RED = 'ff0505'
application = os.getenv('APPLICATION', '')

def error_message(e, account, message):

    error_details = json.loads(e.content.decode())
    error_code = error_details['error']['code']
    error_message = error_details['error']['message']
    
    e_message = {
        "Application": application, 
        "Error Code": error_code,
        "Error Message": message,
        "Project": account,
        "Issue": error_message,
        "Formatted Message": f"*Application*: {application}\n"
                             f"*Account*: {account}\n"
                             f"*Error Code*: {error_code}\n"
                             f"*Error Message*: {message}\n"
                             f"*Issue*: {error_message}"
    }

    return e_message

def send_slack_alert(message, color=RED):
    """
    Send a message to a Slack channel."""
    formatted_message = f"*Calling all* <!here|here>! :rotating_light: *{message['Application']}* has encountered an error! :rotating_light:\n\n{message['Formatted Message']}"
    body = {
        "attachments": [{
            "color": color,  # Ensure this is correctly used
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": formatted_message
                    }
                }
            ]
        }]
    }
    response = requests.post(os.environ['SLACK_WEBHOOK_URL'], data=json.dumps(body), headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise Exception(f"Could not send slack alert: {response.content}")