import os
import logging
import boto3
import botocore.exceptions
import sys
from utils.logger import get_logger
from utils.common_slack import send_slack_alert
from utils.global_vars import error_message
import utils.global_vars as global_vars
from utils.common import list_resource_by_tags


custom_kms_key = "arn:aws:kms:us-east-1:152153062141:key/12983658-2873-48ab-bfe5-53fd5df039fc"

def get_dbs(client):
    """
    Getting a list of existing databases.
    """
    logging.info(f"{global_vars.env} {global_vars.region}: Listing existing DB Instances")
    try:
        dbs = client.describe_db_instances()
        db_with_tags = []
        for db in dbs['DBInstances']:
            if db['KmsKeyId'] != custom_kms_key:
                result = list_resource_by_tags(db, 'DBInstance', client)
                if result:
                    resource_arn, resource_name = result
                    db_with_tags.append({'DBIdentifier': resource_name, 'DBInstanceArn': resource_arn})
    except botocore.exceptions.ClientError as e:
        failure = f"Failed to list DB Instances in region {global_vars.region}"
        slack_alert = error_message(e, global_vars.src_account, failure)
        logging.error(f'{global_vars.env} {global_vars.region}: Error getting DBS.')
        err_message = f'{global_vars.env} {global_vars.region}: {e.response["Error"]["Code"]}: {e.response["Error"]["Message"]}'
        logging.error(err_message)
        send_slack_alert(slack_alert)
        sys.exit(1)

    if not db_with_tags:
        logging.error(f'{global_vars.env} {global_vars.region}: Error getting DBS.')
        logging.error(f"{global_vars.env} {global_vars.region}: Failed to find any DB that match the required tags")
        sys.exit(1)
    else:
        return db_with_tags


def take_snapshot(client, dbs):
    """
    Take snapshots from the list of existing databases.
    """
    snapshots_list = []

    for db in dbs:
        db_name = db['DBIdentifier']
        db_status = client.describe_db_instances(DBInstanceIdentifier=db_name)['DBInstances'][0]['DBInstanceStatus']
        snapshot_name = f"temp-{db_name}-{global_vars.timestamp}"

        logging.info(f"{global_vars.env} {global_vars.region}: Taking snapshot for {db_name}")
        if not global_vars.dry_run:
            if db_status == 'available':
                try:
                    client.create_db_snapshot(
                        DBSnapshotIdentifier=snapshot_name,
                        DBInstanceIdentifier=db_name
                    )
                    snapshots_list.append(snapshot_name)

                except botocore.exceptions.ClientError as err:
                    failure = f"Failing to create snapshot for {db_name}"
                    slack_alert = error_message(err, global_vars.src_account, failure)
                    logging.error(f"{global_vars.env} {global_vars.region}: {failure}.")
                    if err.response["Error"]["Code"] in ['SnapshotQuotaExceeded']:
                        logging.error(
                            f'{global_vars.env} {global_vars.region}: {err.response["Error"]["Code"]}: {err.response["Error"]["Message"]}')
                        send_slack_alert(slack_alert)
                        sys.exit(1)
                    logging.error(
                        f'{global_vars.env} {global_vars.region}: {err.response["Error"]["Code"]}: {err.response["Error"]["Message"]}')
                    send_slack_alert(slack_alert)

    return snapshots_list


def main():
    logger = get_logger(global_vars.log_level)
    if global_vars.wrk_type not in ["main", "dr"]:
        logging.error(f'Value {global_vars.wrk_type} is not supported')
        raise ValueError(f'Value {global_vars.wrk_type} is not supported')
    required_variables = ['SOURCE_ACCOUNT_ID', 'REGION','ENVIRONMENT']
    global_vars.validate(*required_variables)
    rds_client = boto3.client('rds', config=global_vars.client_config)
    dbs = get_dbs(rds_client)
    take_snapshot(rds_client, dbs)


if __name__ == "__main__":
    main()
