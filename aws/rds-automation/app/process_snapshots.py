from utils.logger import get_logger
from utils.common import assume_role, list_snapshots, retention_policy
from utils.common_slack import send_slack_alert
from utils.global_vars import error_message
from datetime import datetime, timezone
import utils.global_vars as global_vars

import sys
import logging
import boto3
import botocore.exceptions


def get_copied_snapshots(client, assumed_region):
    """
    This function gets a list of existing copied snapshots that were created today and are in 'available' status.

    :param client: boto3 client to use for this operation
    :param assumed_region: The region to assume the role in
    :return: A list of snapshots with tags
    """
    logging.info(f"{global_vars.env} {assumed_region}: Listing existing copied snapshots")
    snapshots = list_snapshots(client, assumed_region, global_vars.dst_account, 'manual', includeshared=False)
    snapshots_with_tags = []
    today = datetime.now(timezone.utc).date()

    for copy in snapshots:
        if copy['Status'] == 'available':
            snapshot_date = copy['SnapshotCreateTime'].date()
            if snapshot_date == today:
                try:
                    logging.info(
                        f"{global_vars.env} {assumed_region}: Listing tags for snapshot {copy['DBSnapshotIdentifier']}")
                    tags = client.list_tags_for_resource(ResourceName=copy['DBSnapshotArn'])['TagList']
                    if len(tags) < 3:
                        if any(tag['Key'] == 'copy' and tag['Value'] == 'True' for tag in tags):
                            snapshots_with_tags.append({copy['DBSnapshotIdentifier']: copy})
                except botocore.exceptions.ClientError as e:
                    handle_error(e, f"Failed to process snapshot {copy}")
            else:
                logging.info(f"{global_vars.env} {assumed_region}: Snapshot {copy['DBSnapshotIdentifier']} is not from today")


    if not snapshots_with_tags:
        logging.info(f"{global_vars.env} {assumed_region}: No new copied snapshots to process")
    else:
        logging.info(f"{global_vars.env} {assumed_region}: Found {len(snapshots_with_tags)} snapshots to process.")
    return snapshots_with_tags


def handle_error(e, message):
    """
    This function handles errors by logging the error message and sending a slack alert.
    """
    slack_message = error_message(e, global_vars.dst_account, message=message)
    logging.error(f'{global_vars.env} {global_vars.region}: {message}.')
    logging.error(
        f'{global_vars.env} {global_vars.region}: {e.response["Error"]["Code"]}: {e.response["Error"]["Message"]}')
    send_slack_alert(slack_message)
    sys.exit(1)


def stop_sharing_remote_db(client, snapshot):
    """
    This function stops sharing a remote DB snapshot and deletes it.

    :param client: boto3 client to use for this operation
    :param snapshot: The snapshot to stop sharing
    """
    logging.info(f"Production {global_vars.region}: Stop sharing snapshot {snapshot}")
    try:
        client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot,
                                            AttributeName='restore',
                                            ValuesToRemove=[global_vars.dst_account])
    except botocore.exceptions.ClientError as err:
        handle_snapshot_error(err, snapshot)


def handle_snapshot_error(err, snapshot):
    """
    This function handles snapshot errors by logging the error message and sending a slack alert.

    :param err: The error
    :param snapshot: The snapshot that caused the error
    """
    if err.response['Error']['Code'] == 'DBSnapshotNotFound':
        logging.warning(f"Production {global_vars.region}: Snapshot {snapshot} already removed")
    else:
        slack_alert = error_message(err, global_vars.dst_account, message=f"Failed to stop sharing snapshot {snapshot}")
        logging.error(
            f"""
            {global_vars.env} {global_vars.region}: Failed to stop sharing snapshot {snapshot}. 
            Error message: {err.response["Error"]["Message"]}""")
        send_slack_alert(slack_alert)


def process_snapshots(client, src_client, dr_copies):
    """
    Process snapshots by copying tags from the source snapshot to the destination snapshot and stopping sharing of the source snapshot.

    Args:
        client (boto3.client): The boto3 client to use for the destination operations.
        src_client (boto3.client): The boto3 client to use for the source operations.
        dr_copies (list): A list of dictionaries containing snapshot information to process.
    """
    for copy_dict in dr_copies:
        copy_name, snapshot_info = list(copy_dict.items())[0]
        logging.info(f"{global_vars.env} { global_vars.region}: Processing snapshot {copy_name}")
        source_snapshot_name = snapshot_info['SourceDBSnapshotIdentifier'].split('snapshot:')[1]

        logging.info(f"Production {global_vars.region}: Getting tags from: {source_snapshot_name}")
        if source_snapshot_name in [snapshot['DBSnapshotIdentifier'] for snapshot in list_snapshots(src_client, global_vars.region, global_vars.src_account, 'manual', includeshared=False)]:
            source_tags = src_client.list_tags_for_resource(
                ResourceName=snapshot_info['SourceDBSnapshotIdentifier'])['TagList']
            logging.info(f"{global_vars.env} {global_vars.region}: Setting tags from origin snapshot to: {copy_name}")
            for tag in source_tags:
                client.add_tags_to_resource(ResourceName=snapshot_info['DBSnapshotArn'],
                                            Tags=[{'Key': tag['Key'], 'Value': tag['Value']}])
            client.remove_tags_from_resource(ResourceName=snapshot_info['DBSnapshotArn'], TagKeys=['copy', 'name'])
            client.add_tags_to_resource(ResourceName=snapshot_info['DBSnapshotArn'],
                                        Tags=[{'Key': 'Name', 'Value': copy_name}])
            stop_sharing_remote_db(src_client, source_snapshot_name)
        else:
            logging.warning(f"{global_vars.env} {global_vars.region}: skipping snapshot {source_snapshot_name}")


def main():
    logger = get_logger(global_vars.log_level)
    required_variables = ['REGION', 'DST_ACCOUNT_ID', 'ROLE_NAME', 'ENVIRONMENT']
    global_vars.validate(*required_variables)
    rds_client = boto3.client('rds', config=global_vars.client_config)
    dst_rds_client = assume_role(
        global_vars.dst_account, global_vars.role_name, 'rds', global_vars.region, global_vars.client_config)

    dr_copies = get_copied_snapshots(dst_rds_client, global_vars.region)
    if len(dr_copies) == 0:
        logger.info("No snapshots were found.")
        sys.exit(0)
    process_snapshots(dst_rds_client, rds_client, dr_copies)
    retention_policy(rds_client, 'copy-', global_vars.num_retention)


if __name__ == "__main__":
    main()