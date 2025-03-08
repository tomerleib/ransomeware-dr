from utils.logger import get_logger
from utils.common import get_kms_key, list_snapshots, copy_snapshots, retention_policy
import utils.global_vars as global_vars
from utils.global_vars import dry_run as dry_run

import logging
import boto3
import botocore.exceptions


def process_temp(client, kms_client, snapshots):
    """
    This function processes temporary snapshots. It lists all temporary snapshots, checks their status and tags,
    and if the conditions are met, it creates a copy of the snapshot and adds a 'copied' tag to it.

    :param client: boto3 client to use for this operation
    :param kms_client: boto3 kms client to use for this operation
    """
    kms_arn = get_kms_key(kms_client, global_vars.region)
    processed_snapshots = set()

    if len(snapshots) > 0:
        logging.info(f"There are {len(snapshots)} temporary snapshots available")
        for snapshot in snapshots:
            snapshot_id = snapshot['DBSnapshotIdentifier']
            if snapshot_id not in processed_snapshots:
                temp_snapshots = client.list_tags_for_resource(ResourceName=snapshot['DBSnapshotArn'])['TagList']
                if not any(tag['Key'] == 'copied' and tag['Value'] == 'True' for tag in temp_snapshots):
                    if snapshot['Status'] == 'available':
                        logging.info(f"{global_vars.env} {global_vars.region}: Snapshot {snapshot_id} is available")
                        copied_snapshot = snapshot_id.replace("temp", "copy")
                        logging.info(f"{global_vars.env} {global_vars.region}: Creating a copy: {copied_snapshot}")
                        if not dry_run:
                            snapshot_params = {
                                'client': client,
                                'region': global_vars.region,
                                'source_snapshot_identifier': snapshot_id,
                                'account': global_vars.src_account,
                                'target_snapshot_identifier': copied_snapshot,
                                'kms_key_id': kms_arn,
                                'copy_tags': True
                            }

                            copy_snapshots(**snapshot_params)

                            try:
                                logging.info(
                                    f"{global_vars.env} {global_vars.region}: Adding tags to snapshot {snapshot_id}")
                                client.add_tags_to_resource(ResourceName=snapshot['DBSnapshotArn'],
                                                            Tags=[{'Key': 'copied', 'Value': 'True'}])
                                processed_snapshots.add(snapshot_id)
                            except botocore.exceptions.ClientError as e:
                                logging.error(
                                    f"Could not add tags to resource {snapshot_id}: {e.response['Error']['Message']}")
                        else:
                            logging.info(f"DRY RUN: Snapshot {snapshot_id} should have been copied")
                            logging.info(f"{snapshot['TagList']}")
                    else:
                        logging.info(
                            f"{global_vars.env} {global_vars.region}: Snapshot {snapshot_id} is not available, "
                            f"skipping copy")

            else:
                if dry_run:
                    logging.info(f"snapshot {snapshot['DBSnapshotIdentifier']} was not processed")
                    logging.info(f"{snapshot['TagList']}")
                logging.info(f"{global_vars.env} {global_vars.region}: No snapshots available without tags")

    else:
        logging.info(f"{global_vars.env} {global_vars.region}: No new Temp snapshots available")


def process_copies(client, snapshots):
    """
    This function processes copied snapshots. It lists all the copied snapshots, checks their status and tags,
    and if the conditions are met, it adds a 'shared' tag to the snapshot and shares it with the DR account.

    Args:
        client (boto3.client): The boto3 client to use for this operation.
        snapshots (list): A list of snapshots to process.
    """
    processed_snapshots = set()

    if len(snapshots) > 0:
        logging.info(f"There are {len(snapshots)} copied snapshots available")
        for snapshot in snapshots:
            snapshot_id = snapshot['DBSnapshotIdentifier']
            if snapshot_id not in processed_snapshots:
                if snapshot['Status'] == 'available':
                    source = snapshot['SourceDBSnapshotIdentifier']
                    source_db = source.split('snapshot:')[1]
                    try:
                        copied_snapshots = client.list_tags_for_resource(ResourceName=snapshot['DBSnapshotArn'])[
                            'TagList']
                    except botocore.exceptions.ClientError as e:
                        logging.warning(
                            f"Could not retrieve tags for resource {snapshot_id}: {e.response['Error']['Message']}")
                        continue
                    if not any(tag['Key'] == 'shared' and tag['Value'] == 'True' for tag in copied_snapshots):
                        if not dry_run:
                            logging.info(
                                f"{global_vars.env} {global_vars.region}: Snapshot copy: {snapshot_id} is available")
                            try:
                                logging.info(
                                    f"{global_vars.env} {global_vars.region}: Adding tags to copied snapshot: {snapshot_id}")
                                client.add_tags_to_resource(ResourceName=snapshot['DBSnapshotArn'],
                                                            Tags=[{'Key': 'shared', 'Value': 'True'}])
                            except botocore.exceptions.ClientError as e:
                                logging.error(
                                    f"Could not add tags to resource {snapshot_id}: {e.response['Error']['Message']}")
                                continue
                        logging.info(
                            f"{global_vars.env} {global_vars.region}: Sharing {snapshot_id} with DR account {global_vars.dst_account}")
                        client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot_id,
                                                            AttributeName='restore',
                                                            ValuesToAdd=[global_vars.dst_account])
                        processed_snapshots.add(snapshot_id)
                    else:
                        logging.info(f"Copied snapshot {snapshot_id} could be processed")

                else:
                    if dry_run:
                        logging.info(f"Process Copies: {snapshot_id} {snapshot['TagList']}")
                    logging.info(
                        f"{global_vars.env} {global_vars.region}: Snapshot copy {snapshot_id} is not available")

    else:
        logging.info(f"{global_vars.env} {global_vars.region}: No new copied snapshots available")


def main():
    logger = get_logger(global_vars.log_level)
    required_variables = ['ENVIRONMENT', 'SOURCE_ACCOUNT_ID', 'REGION', 'DST_ACCOUNT_ID']
    global_vars.validate(*required_variables)
    rds_client = boto3.client('rds', config=global_vars.client_config)
    kms_client = boto3.client('kms', config=global_vars.client_config)
    temps = list_snapshots(rds_client, global_vars.region, global_vars.src_account, 'manual', includeshared=False,
                           prefix='temp-', unfiltered=False)
    copies = list_snapshots(rds_client, global_vars.region, global_vars.src_account, 'manual', includeshared=False,
                            prefix='copy-', unfiltered=False)
    process_temp(rds_client, kms_client, temps)
    process_copies(rds_client, copies)
    retention_policy(rds_client, 'temp-', global_vars.num_retention)


if __name__ == "__main__":
    main()
