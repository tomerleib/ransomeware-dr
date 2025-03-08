import os
import json
import requests

RED = 'ff0505'


def send_slack_alert(message, color=RED):
    """
    Send a message to a Slack channel."""
    body = {
        "attachments": [{
            "color": f"{color}",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Calling all*: <!here>\n"
                                f"*Application*: {message['Application']}\n\n"
                                f"*Account*: {message['Account']}\n"
                                f"*Environment*: {message['Environment']}\n"
                                f"*Region*: {message['Region']}\n\n"
                                f"*Error Code*: {message['Error Code']}\n"
                                f"*Error Message*: {message['Error Message']}\n"
                                f"*Issue*: {message['Issue']}"
                    }
                }
            ]
        }]
    }
    response = requests.post(os.environ['SLACK_WEBHOOK_URL'], data=json.dumps(body))
    if response.status_code != 200:
        raise Exception(f"Could not send slack alert: {response.content}")