from utils.logger import get_logger
from utils.common import get_kms_key, assume_role, list_snapshots, copy_snapshots
import utils.global_vars as global_vars

import logging
import botocore.exceptions
import sys
import boto3


def copy_shared_snapshots(client, kms_client, new_snapshots, copy_tags=False):
    """
    Copy a DB snapshot from the source client to the destination client.
    Listing existing shared snapshots and comparing them to the new snapshots to copy.

    :param client: boto3 client to use for this operation on the source or destination client
    :param copy_tags: Whether to copy tags from the snapshot or not
    :param kms_client: KMS client to use for this operation.
    :param new_snapshots: List of new identified snapshots to copy.
    """
    dr_copies = {}
    logging.info(f"{global_vars.env} {global_vars.region}: Listing existing shared snapshots")
    existing_snapshots = list_snapshots(client, global_vars.region, global_vars.dst_account, 'manual', includeshared=False)
    existing_snapshot_names = set(snapshot['DBSnapshotIdentifier'] for snapshot in existing_snapshots)

    new_snapshot_names = set(f"{snapshot['DBSnapshotIdentifier'].split('snapshot:')[1].replace('copy-', '')}" for snapshot in new_snapshots)

    unique_snapshot_names = new_snapshot_names - existing_snapshot_names

    if not unique_snapshot_names:
        logging.info(f"{global_vars.env} {global_vars.region}: No new unique snapshots to copy.")
        return dr_copies

    for snapshot_name in unique_snapshot_names:
        snapshot = next(snapshot for snapshot in new_snapshots if f"{snapshot['DBSnapshotIdentifier'].split('snapshot:')[1].replace('copy-', '')}" == snapshot_name)
        logging.info(f"{global_vars.env} {global_vars.region}: Copying snapshot {snapshot['DBSnapshotIdentifier']}")
        result = copy_snapshots(client, global_vars.region,
                                snapshot['DBSnapshotIdentifier'], snapshot_name,
                                get_kms_key(kms_client, global_vars.region),
                                global_vars.dst_account, copy_tags=copy_tags)
        if result is not None:
            identifier, arn = result
            try:
                logging.info(f"{global_vars.env} {global_vars.region}: Adding tags to snapshot {identifier}")
                client.add_tags_to_resource(ResourceName=arn,
                                            Tags=[{'Key': 'copy', 'Value': 'True'}])
            except botocore.exceptions.ClientError as e:
                logging.warning(f"Could not add tags to resource {identifier}: {e.response['Error']['Message']}")
            dr_copies[identifier] = snapshot
        else:
            logging.warning(
                f"{global_vars.env} {global_vars.region}: Failed to copy snapshot {snapshot['DBSnapshotIdentifier']}")

    return dr_copies


def main():
    logger = get_logger(global_vars.log_level)
    global_vars.validate('REGION', 'DST_ACCOUNT_ID', 'ROLE_NAME', 'ENVIRONMENT')
    dst_kms_client = assume_role(global_vars.dst_account, global_vars.role_name, 'kms', global_vars.region,
                                 global_vars.client_config)
    dst_rds_client = assume_role(global_vars.dst_account, global_vars.role_name, 'rds', global_vars.region,
                                 global_vars.client_config)

    new_snapshots = list_snapshots(
        dst_rds_client, global_vars.region, global_vars.dst_account, 'shared', includeshared=True)
    if len(new_snapshots) == 0:
        logger.info("No snapshots were found.")
        sys.exit(0)
    copy_shared_snapshots(dst_rds_client, dst_kms_client, new_snapshots)



if __name__ == "__main__":
    main()
