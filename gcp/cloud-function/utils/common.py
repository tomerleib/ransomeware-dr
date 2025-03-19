import json
import logging
import time
import os
import requests
from concurrent.futures import as_completed
from datetime import datetime, timezone
from dateutil import parser

from google.auth import default
from googleapiclient.errors import HttpError
import google_auth_httplib2
import httplib2
from googleapiclient import discovery
from googleapiclient import http as googleapiclient_http


RED = 'ff0505'

scopes = ['https://www.googleapis.com/auth/sqlservice.admin']
credentials, project = default()
if not project:
    project = 'production'


def build_request(http, *args, **kwargs):
    new_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
    return googleapiclient_http.HttpRequest(new_http, *args, **kwargs)


authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
service = discovery.build('sqladmin', 'v1beta4', requestBuilder=build_request, http=authorized_http)


def slack_error_message(e, message):
    error_details = json.loads(e.content.decode())
    error_code = error_details['error']['code']
    error_msg = error_details['error']['message']

    e_message = {
        "Application": "GCP Retention",
        "Error Code": error_code,
        "Error Message": message,
        "Project": "production",
        "Issue": error_msg,
        "Formatted Message": f"*Application*: GCP Retention\n"
                             f"*Account*: production\n"
                             f"*Error Code*: {error_code}\n"
                             f"*Error Message*: {message}\n"
                             f"*Issue*: {error_msg}"
    }

    return e_message


def send_slack_alert(message, color=RED):
    """
    Send a message to a Slack channel."""
    formatted_message = (f"*Calling all* <!here|here>! :rotating_light: *{message['GCP Retention']}* has encountered an "
                         f"error! :rotating_light:\n\n{message['Formatted Message']}")
    body = {
        "attachments": [{
            "color": color,
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
    response = requests.post(os.environ['SLACK_WEBHOOK_URL'], data=json.dumps(body),
                             headers={'Content-Type': 'application/json'})
    if response.status_code != 200:
        raise Exception(f"Could not send slack alert: {response.content}")


def get_latest_operation_id(project, instance):
    request = service.operations().list(
        project=project,
        instance=instance
    )
    try:
        response = request.execute(num_retries=5)
        operation_id = response['items'][0]['name']
        return operation_id
    except HttpError as e:
        error_content = json.loads(e.content.decode())
        error_message = error_content['error']['message']
        logging.info(error_message)
        return None


def is_operation_done(project, operation, instance):
    try:
        request = service.operations().get(project=project, operation=operation)
        response = request.execute()
        status = response['status']
        operation_type = response['operationType']
        logging.info(f'The status of the {operation_type} operation for the instance {instance} is: {status}.')
        return status == 'DONE', operation_type, status
    except HttpError as e:
        error_content = json.loads(e.content.decode())
        error_message = error_content['error']['message']
        logging.error(
            f"An error occurred while checking the status of the operation for instance {instance}: {error_message}")
        raise error_message
    except Exception as e:
        logging.error(f"An error occurred while checking the status of the operation for instance {instance}: {str(e)}")
        raise str(e)


def wait_for_operation(project, operation, instance):
    done, operation_type, status = is_operation_done(project, operation, instance)
    while not done:
        logging.info(
            f'Waiting for the {operation_type} operation to complete on the instance {instance}. The current status is: {status}.')
        time.sleep(30)
        done, operation_type, status = is_operation_done(project, operation, instance)


def list_instances(project):
    request = service.instances().list(project=project)
    response = request.execute()
    instances = response['items']
    return instances


def stop_start_instance(project_id, instance, policy):
    """
    Start or stop a GCP instance based on the provided policy.

    Args:
        project_id (str): The ID of the GCP project.
        instance (str): The name of the GCP instance.
        policy (str): The activation policy to apply. Possible values are 'ALWAYS' or 'NEVER'.

    Returns:
        str: The name of the instance that was started or stopped.

    Raises:
        HttpError: If an error occurs while trying to start or stop the instance.
    """
    body = {
        'settings': {
            'activationPolicy': policy
        }
    }
    try:
        if policy == 'ALWAYS':
            logging.info(f"Starting the instance {instance}.")
            request = service.instances().patch(
                project=project_id,
                instance=instance,
                body=body
            )
            response = request.execute()
            logging.info(f"The instance {instance} has been started")
            return response['name']
        else:
            logging.info(f"Stopping the instance {instance}.")
            request = service.instances().patch(
                project=project_id,
                instance=instance,
                body=body
            )
            response = request.execute()
            logging.info(f"The instance {instance} has been stopped")
            return response['name']
    except HttpError as e:
        error_content = json.loads(e.content.decode())
        error_message = error_content['error']['message']
        error_message = f"{instance}: An error occurred while trying to {policy} the instance: {error_message}"
        logging.error(error_message)
        error_message = slack_error_message(e, error_message)
        send_slack_alert(error_message)



def process_instance(project_id, instance, operation_type):
    try:
        latest_operation_id = get_latest_operation_id(project_id, instance['name'])

        request = service.operations().get(project=project_id, operation=latest_operation_id)
        operation = request.execute()

        if operation['status'] != 'DONE':
            logging.info(
                f"Another operation {operation['operationType']} is running on {instance['name']}. Trying again later")
            wait_for_operation(project_id, operation['name'], instance['name'])

        logging.info(f"Processing instance {instance['name']} with operation {operation_type}")
        logging.debug(
            f"Instance {instance['name']} is set with activation policy {instance['settings']['activationPolicy']}")
        if operation_type == 'start':
            logging.debug(f"Instance {instance['name']} is being started'")
            stop_start_instance(project_id, instance['name'], 'ALWAYS')
        elif operation_type == 'stop':
            logging.debug(f"Instance {instance['name']} is being stopped'")
            stop_start_instance(project_id, instance['name'], 'NEVER')
    except Exception as e:
        error_message = f"Error processing instance {instance['name']} during {operation_type} operation: {str(e)}"
        return {"status": "error", "error": error_message}


def process_instance_retention(project_id, instance, days):
    try:
        latest_operation_id = get_latest_operation_id(project_id, instance['name'])

        request = service.operations().get(project=project_id, operation=latest_operation_id)
        operation = request.execute()

        if operation['status'] != 'DONE':
            logging.info(
                f"Another operation {operation['operationType']} is running on {instance['name']}. Trying again later")
            wait_for_operation(project_id, operation['name'], instance['name'])

        logging.info(f"Processing instance {instance['name']} retention ")
        try:
            delete_older_snapshots(instance, days, project_id)
        except HttpError as e:
            error_content = json.loads(e.content.decode())
            error_message = error_content['error']['message']
            logging.error(
                f"An error occurred while deleting snapshots for instance {instance['name']}: {error_message}")
            raise error_message
    except Exception as e:
        error_message = f"Error processing instance {instance['name']} during retention operation: {str(e)}"
        return {"status": "error", "error": error_message}


def handle_futures(futures):
    for future in as_completed(futures):
        result = future.result()
        if result is not None and result['status'] == 'error':
            logging.error({result['error']})
        elif result is not None:
            logging.info(result['message'])


def delete_older_snapshots(instance, days, project=project):
    """
    Deletes snapshots older than x days for a given instance in a GCP project.

    Args:
        instance: The instance for which to delete snapshots.
        days: The number of days to keep snapshots.
        project: The ID of the GCP project.

    Returns:
        None

    Raises:
        HttpError: If an error occurs while deleting snapshots.
    """
    days = int(days)
    logging.info(f"Doing a {days} days retention cleanup")
    all_snapshots = []
    request = service.backupRuns().list(project=project, instance=instance['name'])

    try:
        while request is not None:
            response = request.execute()
            if 'items' in response:
                all_snapshots.extend(response['items'])
            request = service.backupRuns().list_next(previous_request=request, previous_response=response)

        for snapshot in [snapshot for snapshot in all_snapshots if snapshot['status'] == 'SUCCESSFUL']:
            snapshot_time = parser.parse(snapshot['endTime'])
            current_time = datetime.now(timezone.utc)
            age = current_time - snapshot_time
            logging.info(f"Snapshot {snapshot['id']} for instance {instance['name']} is {age.days} days old.")
            if age.days >= days:
                logging.info(f"Deleting snapshot {snapshot['id']} for instance {instance['name']}")
                service.backupRuns().delete(
                    project=project,
                    instance=instance['name'],
                    id=snapshot['id']
                ).execute()
                logging.info(f"Snapshot {snapshot['id']} deleted successfully")
    except HttpError as e:
        error_content = json.loads(e.content.decode())
        error_message = error_content['error']['message']
        error_info = f"An error occurred while deleting snapshots for instance {instance['name']}: {error_message}"
        er = slack_error_message(e, error_info)
        send_slack_alert(er)
        raise RuntimeError(error_info)


def error_message(e, account, failure):
    """
    Format error message for Slack notification.
    """
    error_content = json.loads(e.content.decode())
    error_message = error_content['error']['message']
    error = {
        "Project": "production",
        "Error": error_message,
        "Failure": failure,
        "Message": f"*Account*: production\n"
                  f"*Error*: {error_message}\n"
                  f"*Failure*: {failure}"
    }
    return error