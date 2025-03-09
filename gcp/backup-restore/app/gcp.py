import json
import time
import random
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.auth import default
from googleapiclient.errors import HttpError
from googleapiclient import discovery
from googleapiclient import http as google_http
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type, after_log
import google_auth_httplib2
import httplib2
import logging
from datetime import datetime, timezone

from utils.logger import get_logger
from utils.slack import send_slack_alert
from utils.slack import error_message as slack_error_message

logger = get_logger()
source_project_id = 'production'
target_project_id = 'dr-backup'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/etc/workload-identity/credential-configuration.json'

target_scopes = ['https://www.googleapis.com/auth/sqlservice', 'https://www.googleapis.com/auth/cloud-platform']
credentials, _ = default(scopes=target_scopes)


def build_request(http, *args, **kwargs):
    new_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
    return google_http.HttpRequest(new_http, *args, **kwargs)


authorized_http = google_auth_httplib2.AuthorizedHttp(credentials, http=httplib2.Http())
service = discovery.build('sqladmin', 'v1beta4', requestBuilder=build_request, http=authorized_http, num_retries=5)

retry_on_http_error = retry(
    retry=retry_if_exception_type(HttpError),
    stop=stop_after_attempt(10),
    wait=wait_exponential_jitter(initial=1, max=180),
    after=after_log(logger, logging.WARNING),
    reraise=True
)

def custom_retry(retries=10, initial_wait=1, max_wait=180, exp_base=2, jitter=1):
    """
    A decorator to retry a function call with exponential backoff and jitter.

    Args:
        retries (int): The maximum number of retry attempts.
        initial_wait (int): The initial wait time between retries in seconds.
        max_wait (int): The maximum wait time between retries in seconds.
        exp_base (int): The base of the exponential backoff.
        jitter (int): The maximum random jitter to add to the wait time in seconds.

    Returns:
        function: The decorated function with retry logic.
    """
    def decorator(func):
        """
        A decorator to retry a function call with exponential backoff and jitter.

        Args:
            func (function): The function to be decorated.

        Returns:
            function: The decorated function with retry logic.
        """
        def wrapper(*args, **kwargs):
            """
            Wrapper function to apply retry logic.

            Args:
                *args: Variable length argument list.
                **kwargs: Arbitrary keyword arguments.

            Returns:
                Any: The return value of the decorated function.

            Raises:
                HttpError: If the maximum number of retries is reached.
            """
            attempt = 0
            wait_time = initial_wait
            while attempt < retries:
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    attempt += 1
                    if attempt >= retries:
                        logger.warning(f"Max retries reached for {func.__name__}. Raising exception.")
                        raise
                    else:
                        logger.warning(f"Retrying {func.__name__} due to {e}. Attempt {attempt}/{retries}.")
                        time.sleep(wait_time + random.uniform(0, jitter))
                        wait_time = min(max_wait, wait_time * exp_base)
        return wrapper
    return decorator


@retry_on_http_error
def list_instances(project):
    """
    List all instances in a given GCP project.

    Args:
        project (str): The ID of the GCP project.

    Returns:
        list: A list of instances in the project.
    """
    try:
        request = service.instances().list(
            project=project
        )
        response = request.execute()
        instances = response['items']
        instances_list = [instance for instance in instances if not instance['name'].endswith("-replica")]
        return instances_list
    except HttpError as e:
        error_content = json.loads(e.content.decode())
        error_message = error_content['error']['message']
        error_message = f"Error listing instances in project {project}: {error_message}"
        logging.error(error_message)
        error = slack_error_message(e, project, error_message)
        send_slack_alert(error)
        raise HttpError(error_message)


class SqlInstance:
    """
    A class to represent a SQL instance in GCP.

    Attributes:
        project (str): The ID of the GCP project.
        sqladmin (Resource): The SQL Admin service resource.
        instance (str): The name of the SQL instance.
    """

    def __init__(self, project, instance, dr=False):
        """
        Initialize the SqlInstance object.

        Args:
            project (str): The ID of the GCP project.
            instance (dict): The instance details.
            dr (bool): Flag to indicate if this is a disaster recovery instance.
        """
        self.project = project
        self.sqladmin = service
        self.instance = f"{instance['name']}-rdr" if dr else instance['name']


    @custom_retry()
    def describe_instance(self):
        """
        Retrieve the details of the SQL instance.

        Returns:
            dict: The details of the SQL instance.

        Raises:
            HttpError: If an error occurs while retrieving the instance details.
        """
        try:
            request = self.sqladmin.instances().get(project=self.project, instance=self.instance)
            response = request.execute()
            return response
        except HttpError as e:
            logger.error(f'Error: {e}')


    @custom_retry()
    def stop_start_instance(self, policy):
        """
        Start or stop a GCP instance based on the provided policy.

        Args:
            policy (str): The activation policy to apply. Possible values are 'ALWAYS' or 'NEVER'.

        Returns:
            str: The name of the operation ID.

        Raises:
            HttpError: If an error occurs while trying to start or stop the instance.
        """
        if self.project == 'aol-prod':
            raise ValueError("Modifying resources in the 'aol-prod' project is not allowed.")
        body = {
            'settings': {
                'activationPolicy': policy
            }
        }
        try:
            current_policy = self.describe_instance()['settings']['activationPolicy']
            if current_policy == policy:
                logging.info(f"{self.instance}: The instance is already {'running' if policy == 'ALWAYS' else 'stopped'}")
                return
            logging.info(f"{self.instance}: {'Starting' if policy == 'ALWAYS' else 'Stopping'} the instance")

            request = service.instances().patch(
                project=self.project,
                instance=self.instance,
                body=body
            )
            operation = request.execute()['name']
            logging.info(
                f"{self.instance} - {self.project}: "
                f"Operation to {'START' if policy == 'ALWAYS' else 'STOP'} is running")
            self.get_operation_status(operation, 'START' if policy == 'ALWAYS' else 'STOP', self.instance, self.project)
            return operation
        except HttpError as e:
            error_content = json.loads(e.content.decode())
            error_message = error_content['error']['message']
            error_message = (f"{self.instance} - {self.project}: "
                             f"An error occurred while trying to change the policy to {policy}: {error_message}")
            logging.error(error_message)
            error = slack_error_message(e, self.project, error_message)
            send_slack_alert(error)
            raise HttpError(e.resp, error_message)


    @custom_retry()
    def get_snapshot_status(self, backup_id):
        """
        Check the status of a snapshot until it is completed.

        Args:
            backup_id (str): The ID of the backup to check.

        Raises:
            HttpError: If an error occurs while checking the backup status.
        """
        try:
            logging.info(f"{self.instance} - {self.project}: Checking backup status of {backup_id}")
            backup_status = service.backupRuns().get(
                project=self.project, instance=self.instance, id=backup_id).execute()
            status = backup_status['status']
            while status != 'SUCCESSFUL':
                logging.info(f"{self.instance} - {self.project}: Backup status: {status}")
                time.sleep(30)
                backup_status = service.backupRuns().get(
                    project=self.project, instance=self.instance, id=backup_id).execute()
                status = backup_status['status']
                if status == 'FAILED':
                    backup_error = backup_status['error']
                    logging.error(f"{self.instance} - {self.project}: Backup {backup_id} failed: {backup_error}")
                    raise HttpError(f"{self.instance} - {self.project}: Backup {backup_id} failed: {backup_error}")
            logging.info(f"{self.instance} - {self.project}: Backup {backup_id} completed successfully")
        except HttpError as e:
            error_content = json.loads(e.content.decode())
            error_message = error_content['error']['message']
            logging.error(f"{self.instance} - {self.project}: Error checking backup status: {error_message}")

    @custom_retry()
    def get_operation_status(self, operation_id, operation_type, instance_id, project_id):
        """
        Check the status of a given operation until it is completed.

        Args:
            operation_id (str): The ID of the operation to check.
            operation_type (str): The type of the operation (e.g., 'START', 'STOP', 'backup', 'restore').
            instance_id (str): The ID of the instance associated with the operation.
            project_id (str): The ID of the GCP project.

        Raises:
            HttpError: If an error occurs while checking the operation status or if the operation fails.
        """
        max_retries = 5
        retry_delay = 30  # seconds
        retries = 0

        while retries < max_retries:
            try:
                logging.info(f"{instance_id} - {project_id}: "
                             f"Checking operation status of {operation_id}, type: {operation_type}")
                operation_status = service.operations().get(project=project_id, operation=operation_id).execute()
                status = operation_status['status']
                while status != 'DONE':
                    logging.info(f"{instance_id} - {project_id}: Operation status of {operation_type}: {status}")
                    time.sleep(30)
                    operation_status = service.operations().get(project=project_id, operation=operation_id).execute()
                    status = operation_status['status']
                    if status == 'FAILED':
                        operation_error = operation_status['error']
                        logging.error(f"{instance_id} - {project_id}: "
                                      f"Operation {operation_id} of {operation_type} failed: {operation_error}")
                        raise HttpError(f"{instance_id} - {project_id}: "
                                        f"Operation {operation_id} of {operation_type} failed: {operation_error}")
                logging.info(f"{instance_id} - {project_id}: "
                             f"Operation {operation_id} of {operation_type} completed successfully")
                return
            except HttpError as e:
                    error_content = json.loads(e.content.decode())
                    error_message = error_content['error']['message']
                    logging.error(f"{instance_id} - {project_id}: "
                                  f"Error checking operation status of {operation_type}: {error_message}")
                    raise HttpError


    @custom_retry()
    def create_snapshot(self):
        """
        Create a snapshot of the SQL instance.

        This method creates a snapshot of the SQL instance if it is running.
        It checks the current activation policy of the instance and proceeds
        to create a snapshot if the instance is not stopped. The method also
        monitors the status of the snapshot creation operation.

        Returns:
            str: The backup ID of the created snapshot, or None if the instance is stopped.

        Raises:
            HttpError: If an error occurs while creating the snapshot.
        """
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M')
        body = {
            'description': f"Snapshot for {self.instance} on {timestamp}",
            'instance': self.instance,
            'project': self.project
        }
        try:
            current_policy = self.describe_instance()['settings']['activationPolicy']
            if current_policy == 'NEVER':
                logging.error(f"{self.instance}: The instance is stopped. Cannot create a snapshot.")
                return None
            logging.info(f"{self.instance}: Creating a snapshot of the instance")
            request = service.backupRuns().insert(
                project=self.project,
                instance=self.instance,
                body=body
            )
            response = request.execute()
            logging.info(f"{self.instance}: Operation to create a snapshot is running...")
            backup_id = response['backupContext']['backupId']
            self.get_snapshot_status(backup_id)
            operation_id = response['name']
            self.get_operation_status(operation_id, 'backup', self.instance, self.project)
            return backup_id
        except HttpError as e:
            error_content = json.loads(e.content.decode())
            error_message = error_content['error']['message']
            logging.error(f"{self.instance} - {self.project}: Error taking snapshot: {error_message}")
            error = slack_error_message(e, self.project, f"Error taking snapshot of {self.instance}: {error_message}")
            send_slack_alert(error)


    @custom_retry()
    def restore_backup(self, backup_id, target_project):
        """
        Restore a backup to a target instance.

        Args:
            backup_id (str): The ID of the backup to restore.
            target_project (str): The ID of the target project where the backup will be restored.

        Returns:
            str: The name of the restore operation.

        Raises:
            HttpError: If an error occurs while restoring the backup.
        """
        if target_project == 'aol-prod':
            raise ValueError("Modifying resources in the 'aol-prod' project is not allowed.")
        target_instance = f"{self.instance}-rdr"
        try:
            target_instance_obj = SqlInstance(target_project, {'name': target_instance})
            target_instance_obj.stop_start_instance('ALWAYS')

            logging.info(f"{target_instance} - {target_project}: Restoring backup {backup_id}")
            logging.info(f"{target_instance} - {target_project}: Restoring backup from {self.instance} in project "
                         f"{self.project}")

            restore_operation = {
                'restoreBackupContext': {
                    'backupRunId': backup_id,
                    'project': self.project,
                    'instanceId': self.instance
                }
            }

            request = service.instances().restoreBackup(
                project=target_project,
                instance=target_instance,
                body=restore_operation
            )

            response = request.execute()
            operation = response['name']

            logging.info(
                f"{target_instance} - {target_project}: Restore operation started")
            self.get_operation_status(operation, 'restore', target_instance, target_project)
            return operation

        except HttpError as e:
            error_content = json.loads(e.content.decode())
            error_message = error_content['error']['message']
            error_message = (f"{target_instance} - {target_project}: "
                             f"Error attempting to restore {backup_id}: {error_message}")
            logging.error(error_message)
            error = slack_error_message(e, target_project, error_message)
            send_slack_alert(error)


    @custom_retry()
    def cleanup_snapshots(self, backup_id):
        """
        Delete a snapshot of the SQL instance.

        Args:
            backup_id (str): The ID of the backup to delete.

        Raises:
            HttpError: If an error occurs while deleting the snapshot.
        """
        logging.info(f"{self.instance} - {self.project}: Deleting snapshot {backup_id}")
        try:
            service.backupRuns().delete(
                project=self.project,
                instance=self.instance,
                id=backup_id).execute()
        except HttpError as e:
            error_content = json.loads(e.content.decode())
            error_message = error_content['error']['message']
            error_message = f"{self.instance} - {self.project}: Error deleting snapshot {backup_id}: {error_message}"
            logging.error(error_message)
            error = slack_error_message(e, self.project, error_message)
            send_slack_alert(error)


def backup_restore(source_project, target_project, instance):
    """
    Perform backup and restore operations for a given SQL instance.

    Args:
        source_project (str): The ID of the source GCP project.
        target_project (str): The ID of the target GCP project.
        instance (dict): The instance details.

    Returns:
        None
    """
    sql_instance = SqlInstance(source_project, instance)
    backup_id = sql_instance.create_snapshot()
    if backup_id:
        sql_instance.restore_backup(backup_id, target_project)
        sql_instance.cleanup_snapshots(backup_id)
    else:
        logging.error(f"Instance {instance['name']} is stopped. Cannot create a snapshot.")


def dr_backup(target_project, instance):
    """
    Perform backup operations for a given SQL instance.

    Args:
        target_project (str): The ID of the target GCP project.
        instance (dict): The instance details.

    Returns:
        None
    """
    dr_instance = SqlInstance(target_project, instance, dr=True)
    backup_id = dr_instance.create_snapshot()
    if backup_id:
        dr_instance.stop_start_instance('NEVER')
    else:
        logging.error(f"{instance} - {target_project}: Failed to create a backup.")


def main():
    start = time.time()
    source_instances = list_instances(source_project_id)
    target_instances = list_instances(target_project_id)
    target_instance_names = [instance['name'] for instance in target_instances]
    prod_instances = [instance for instance in source_instances if
                              f"{instance['name']}-rdr" in target_instance_names]

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(backup_restore, source_project_id, target_project_id, instance): instance for
                   instance in prod_instances}

        for future in as_completed(futures):
            instance = futures[future]
            try:
                future.result()
                logging.info(f"Instance {instance['name']} processed successfully")
            except Exception as e:
                logging.error(f"Error processing instance {instance['name']}: {str(e)}")

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(dr_backup, target_project_id, instance): instance for instance in prod_instances}

        for future in as_completed(futures):
            instance = futures[future]
            try:
                future.result()
                logging.info(f"Instance {instance['name']} processed successfully")
            except Exception as e:
                logging.error(f"Error processing instance {instance['name']}: {str(e)}")


if __name__ == '__main__':
    main()