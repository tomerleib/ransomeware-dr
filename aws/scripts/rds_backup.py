import logging
import os
import boto3
import botocore.exceptions
import time

from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
src_account = os.getenv('SOURCE_ACCOUNT_ID')
region = os.getenv('REGION') or input("Enter source region: ") or 'us-west-1'
src_key = os.getenv('SOURCE_AWS_ACCESS_KEY_ID')
src_secret = os.getenv('SOURCE_AWS_SECRET_ACCESS_KEY')
dst_account = os.getenv('DST_ACCOUNT_ID')
dst_key = os.getenv('DST_AWS_ACCESS_KEY_ID')
dst_secret = os.getenv('DST_AWS_SECRET_ACCESS_KEY')
timestamp = datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
kms = 'arn:aws:kms:us-west-1:762952282510:key/93b30e2b-e4f7-4ee3-9e91-eb15b720dd48'  ## Todo: make it region aware instead of hardcoded.


src_rds_client = boto3.session.Session(aws_access_key_id=src_key,
                                       aws_secret_access_key=src_secret,
                                       region_name=region).client('rds')

dst_rds_client = boto3.session.Session(aws_access_key_id=dst_key,
                                       aws_secret_access_key=dst_secret,
                                       region_name=region).client('rds')


def get_kms_key():
    try:
        source_kms_client = boto3.session.Session(aws_access_key_id=src_key,
                                                  aws_secret_access_key=src_secret,
                                                  region_name=region).client('kms')
        dest_kms_client = boto3.session.Session(aws_access_key_id=dst_key,
                                                aws_secret_access_key=dst_secret,
                                                region_name=region).client('kms')
        source_kms_arn = source_kms_client.describe_key(KeyId='alias/jfrog-rds')['KeyMetadata']['Arn']
        dest_kms_arn = dest_kms_client.describe_key(KeyId='alias/jfrog-rds')['KeyMetadata']['Arn']
        return source_kms_arn, dest_kms_arn
    except botocore.exceptions.ClientError as e:
        logging.error("Error getting KMS key. Here's why: %s: %s",
                      e.response["Error"]["Code"],
                      e.response["Error"]["Message"])
        raise
    except Exception as e:
        logging.error("Unexpected error getting KMS key: %s", e)
        raise


def get_dbs():
    try:
        dbs = src_rds_client.describe_db_instances()

        db_with_tags = []
        for db in dbs['DBInstances']:
            db_tags = src_rds_client.list_tags_for_resource(ResourceName=db['DBInstanceArn'])['TagList']

            if any(tag['Key'] == 'workload_type' and tag['Value'] == 'dr' for tag in db_tags):
                db_with_tags.append({'DBIdentifier': db['DBInstanceIdentifier'], 'DBInstanceArn': db['DBInstanceArn']})
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return False
    return db_with_tags


def copy_db_snapshot(client, source_snapshot_identifier, target_snapshot_identifier, kms_key_id, copy_tags=False):
    """
    Copy a DB snapshot from the source client to the destination client.

    :param client: boto3 client to use for this operation on the source or destination client
    :param copy_tags: Whether to copy tags from the snapshot or not
    :param source_snapshot_identifier: Source DB snapshot name
    :param target_snapshot_identifier: Target DB snapshot name
    :param kms_key_id: KMS Key ID
    :return: The target snapshot ARN
    """
    try:
        copy_response = client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=source_snapshot_identifier,
            TargetDBSnapshotIdentifier=target_snapshot_identifier,
            KmsKeyId=kms_key_id,
            CopyTags=copy_tags
        )
        target_snapshot_arn = copy_response['DBSnapshot']['DBSnapshotArn']
        r = client.describe_db_snapshots(DBSnapshotIdentifier=target_snapshot_identifier)
        while r['DBSnapshots'][0]['Status'] != 'available':
            time.sleep(10)
            r = client.describe_db_snapshots(DBSnapshotIdentifier=target_snapshot_identifier)
            logging.debug(f"{r['DBSnapshots'][0]['Status']}")
            logging.info(f"Snapshot {target_snapshot_identifier} copy is in progress... ")
        logging.info(f"Snapshot {target_snapshot_identifier} is available")
        return target_snapshot_arn

    except botocore.exceptions.ClientError as e:
        logging.error(f"Error copying snapshot: {e}")
        raise


def take_snapshot(dbs):
    snapshots_and_arns = {}
    cp_snapshots = []

    for db in dbs:
        db_name = db['DBIdentifier']
        snapshot_name = f"temp-{db_name}-{timestamp}"
        copied_snapshot = f"copy-{db_name}-{timestamp}"
        logging.info(f"Taking snapshot of {db_name}")

        try:
            src_rds_client.create_db_snapshot(
                DBSnapshotIdentifier=snapshot_name,
                DBInstanceIdentifier=db_name
            )

            rds_waiter = src_rds_client.get_waiter('db_snapshot_available')
            rds_waiter.wait(
                DBSnapshotIdentifier=snapshot_name,
                WaiterConfig={'Delay': 10, 'MaxAttempts': 60}
            )
            logging.info(f"Snapshot {snapshot_name} is available")

            snapshots_and_arns[db_name] = copy_db_snapshot(client=src_rds_client,
                                                           source_snapshot_identifier=snapshot_name,
                                                           target_snapshot_identifier=copied_snapshot,
                                                           kms_key_id=kms, copy_tags=True)
            cp_snapshots.append(copied_snapshot)

            src_rds_client.modify_db_snapshot_attribute(DBSnapshotIdentifier=copied_snapshot,
                                                        AttributeName='restore',
                                                        ValuesToAdd=[dst_account])

        except botocore.exceptions.ClientError as err:
            logging.error(
                "Couldn't get snapshot %s. Here's why: %s: %s",
                copied_snapshot,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise
        except Exception as e:
            logging.error(f"Error taking snapshot of {snapshot_name}: {e}")

    return snapshots_and_arns, cp_snapshots


def copy_shared(snapshots, dest_kms_arn):
    logging.info("Copying shared...")
    for snapshot_name, snapshot_arn in snapshots.items():
        snapshot_name = f"{snapshot_name}-{timestamp}"
        copy_db_snapshot(client=dst_rds_client, source_snapshot_identifier=snapshot_arn,
                         target_snapshot_identifier=snapshot_name, kms_key_id=dest_kms_arn)


def stop_sharing_db(snapshots, client):
    logging.info("Stopping shared DBs")
    for snapshot_name in snapshots:
        try:
            client.modify_db_snapshot_attribute(DBSnapshotIdentifier=snapshot_name,
                                                AttributeName='restore',
                                                ValuesToRemove=[dst_account])
        except botocore.exceptions.ClientError as err:
            logging.error(
                "Failed to stop sharing snapshot: %s. Here's why: %s: %s",
                snapshot_name,
                err.response["Error"]["Code"],
                err.response["Error"]["Message"],
            )
            raise


def main():
    source_kms_arn, dest_kms_arn = get_kms_key()
    dbs = get_dbs()
    snapshots, cp_snapshots = take_snapshot(dbs)
    copy_shared(snapshots, dest_kms_arn)
    stop_sharing_db(cp_snapshots, client=src_rds_client)


if __name__ == "__main__":
    main()
