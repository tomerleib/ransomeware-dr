import logging
import boto3
import botocore.exceptions
import sys
import time
from .common_slack import send_slack_alert
from .global_vars import error_message, env,wrk_type, ba_keys, dry_run, rds_service_values
from .global_vars import group as group_tag

max_attempts = 120
connection_max_attempts = 60


def assume_role(account_id, role, resource_type, assumed_region, config):
    """
    Assume a role in the specified account and return a client for the specified resource type."""
    sts_client = boto3.client('sts', config=config)
    role_arn = f'arn:aws:iam::{account_id}:role/{role}'

    try:
        assumed_role = sts_client.assume_role(
            RoleArn=role_arn,
            ExternalId='rds-automation',
            RoleSessionName='rds-automation'
        )
        session = boto3.Session(
            aws_access_key_id=assumed_role['Credentials']['AccessKeyId'],
            aws_secret_access_key=assumed_role['Credentials']['SecretAccessKey'],
            aws_session_token=assumed_role['Credentials']['SessionToken'],
            region_name=assumed_region
        )
        return session.client(resource_type, config=config)
    except botocore.exceptions.ClientError as e:
        logging.error(f'Production {assumed_region}: There was an error assuming the role: {role_arn}')
        logging.error(f"Production {assumed_region}: {e.response['Error']['Code']}: {e.response['Error']['Message']}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Production {assumed_region}: Unexpected error assuming role in account {account_id}: {e}")
        sys.exit(1)


def handle_error(err, environment, region, failure_message, slack_alert):
    """
    Handle errors by logging the failure message and sending a Slack alert.

    Args:
        err (botocore.exceptions.ClientError): The error object containing details of the error.
        environment (str): The environment in which the error occurred (e.g., 'Production').
        region (str): The AWS region where the error occurred.
        failure_message (str): A custom message describing the failure.
        slack_alert (str): The Slack alert message to be sent.

    Logs:
        Logs the failure message and the error details.

    Sends:
        Sends a Slack alert with the provided message.
    """
    logging.error(f"{environment} {region}: {failure_message}.")
    logging.error(
        f"{environment} {region}: Here's why: {err.response['Error']['Code']}: {err.response['Error']['Message']}")
    if not dry_run:
        send_slack_alert(slack_alert)


def get_kms_key(client, region):
    """
    Getting KMS keys.
    """
    try:
        kms_arn = client.describe_key(KeyId='alias/jfrog-rds')['KeyMetadata']['Arn']
        return kms_arn
    except botocore.exceptions.ClientError as err:
        logging.error(
            f"Production {region}: Failed to retrieve KMS info. Here's why: {err.response['Error']['Code']}: {err.response['Error']['Message']}")
        sys.exit(1)


def get_tags_by_type(resource, resource_type, client):
    """
    Retrieve tags for a given resource based on its type.

    Args:
        resource (dict): The resource for which to retrieve tags.
        resource_type (str): The type of the resource ('DBInstance' or 'DBSnapshot').
        client (boto3.client): The boto3 client to use for the operation.

    Returns:
        list: A list of tags associated with the resource.
    """
    if resource_type == 'DBInstance':
        db_tags = client.list_tags_for_resource(ResourceName=resource['DBInstanceArn'])['TagList']
        if dry_run:
            logging.debug(f"{resource['DBInstanceIdentifier']} tags: {db_tags}")
            time.sleep(1)
        return db_tags
    elif resource_type == 'DBSnapshot':
        snapshot_tags = client.list_tags_for_resource(ResourceName=resource['DBSnapshotArn'])['TagList']
        if dry_run:
            logging.debug(f"{resource['DBSnapshotIdentifier']} tags: {snapshot_tags}")
        return snapshot_tags
    return []


def filter_tags(resource_tags):
    """
    Check if the resource tags match the required criteria.

    Args:
        resource_tags (list): A list of tags associated with the resource.

    Returns:
        bool: True if the tags match the criteria, False otherwise.
    """
    if not group_tag:
        return (
            any(tag['Key'] == 'workload_type' and tag['Value'] == wrk_type for tag in resource_tags) and
            any(tag['Key'] == 'service' and tag['Value'] in rds_service_values for tag in resource_tags) and
            any(tag['Key'] in ['name','Name'] and 'byok' not in tag['Value'] for tag in resource_tags) and
            not any(tag['Key'] == 'exclude_rdr' and tag['Value'] == 'True' for tag in resource_tags) and
            not any(tag['Key'] == 'custom_kms_key' and tag['Value'] == 'true' for tag in resource_tags) and
            not any(tag['Key'] in ba_keys and tag['Value'] == 'ba' for tag in resource_tags)
        )
    else:
        return (
            any(tag['Key'] in ba_keys and tag['Value'] == 'ba' for tag in resource_tags) and
            any(tag['Key'] == 'service' and tag['Value'] in rds_service_values for tag in resource_tags) and
            any(tag['Key'] == 'workload_type' and tag['Value'] == wrk_type for tag in resource_tags) and
            any(tag['Key'] in ['name','Name'] and 'byok' not in tag['Value'] for tag in resource_tags) and
            not any(tag['Key'] == 'exclude_rdr' and tag['Value'] == 'True' for tag in resource_tags) and
            not any(tag['Key'] == 'custom_kms_key' and tag['Value'] == 'true' for tag in resource_tags)
        )


def list_resource_by_tags(resource, resource_type, client):
    """
    List resources by their tags.

    Args:
        resource (dict): The resource to check.
        resource_type (str): The type of the resource ('DBInstance' or 'DBSnapshot').
        client (boto3.client): The boto3 client to use for the operation.

    Returns:
        tuple or dict or None: A tuple containing the resource ARN and identifier if the resource type is 'DBInstance',
                               the resource itself if the resource type is 'DBSnapshot', or None if the tags do not match.
    """
    resource_tags = get_tags_by_type(resource, resource_type, client)
    if filter_tags(resource_tags):
        if resource_type == 'DBInstance':
            return resource['DBInstanceArn'], resource['DBInstanceIdentifier']
        elif resource_type == 'DBSnapshot':
            return resource
    return None


def list_snapshots(client, region, account, snapshot_type, includeshared=False, prefix=None, unfiltered=True):
    """
    List RDS snapshots based on the specified criteria.

    Args:
        client (boto3.client): The boto3 client to use for the operation.
        region (str): The AWS region where the snapshots are located.
        account (str): The AWS account ID.
        snapshot_type (str): The type of snapshots to list (e.g., 'manual', 'automated').
        includeshared (bool, optional): Whether to include shared snapshots. Defaults to False.
        prefix (str, optional): A prefix to filter the snapshot identifiers. Defaults to None.

    Returns:
        list: A list of filtered snapshots that match the criteria.
    """
    try:
        paginator = client.get_paginator('describe_db_snapshots')
        response_iterator = paginator.paginate(
            IncludeShared=includeshared,
            SnapshotType=snapshot_type
        )

        snapshots = [snapshot for page in response_iterator for snapshot in page['DBSnapshots']]

        if prefix:
            snapshots = [snapshot for snapshot in snapshots if snapshot['DBSnapshotIdentifier'].startswith(prefix)]

        if includeshared:
            return snapshots

        if unfiltered:
            return snapshots

        filtered_snapshots = [snapshot for snapshot in snapshots if
                              list_resource_by_tags(snapshot, 'DBSnapshot', client)]

        if dry_run:
            for snapshot in filtered_snapshots:
                logging.info(f"snapshot: {snapshot['DBSnapshotIdentifier']} is filtered")

        return filtered_snapshots


    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'Throttling':
            time.sleep(60)
            return list_snapshots(client, region, account, includeshared, snapshot_type, prefix)
        else:
            slack_alert = error_message(e, account, f"Failed to list {snapshot_type} snapshots in {region}")
            logging.error(
                f'{env} {region}: Failed to list shared snapshots . Error message: {e.response["Error"]["Message"]}')
            if not dry_run:
                send_slack_alert(slack_alert)
            sys.exit(1)


def copy_snapshots(
        client, region, source_snapshot_identifier, target_snapshot_identifier, kms_key_id, account,
        copy_tags=False):
    """
    Copy a DB snapshot to replace the default KMS key.

    :param client: boto3 client to use for this operation on the source or destination client
    :param copy_tags: Whether to copy tags from the snapshot or not
    :param source_snapshot_identifier: Source DB snapshot name
    :param target_snapshot_identifier: Target DB snapshot name
    :param kms_key_id: KMS Key ID
    """
    try:
        copy_response = client.copy_db_snapshot(
            SourceDBSnapshotIdentifier=source_snapshot_identifier,
            TargetDBSnapshotIdentifier=target_snapshot_identifier,
            KmsKeyId=kms_key_id,
            CopyTags=copy_tags
        )

        target_snapshot_identifier = copy_response['DBSnapshot']['DBSnapshotIdentifier']
        arn = copy_response['DBSnapshot']['DBSnapshotArn']

        return target_snapshot_identifier, arn

    except botocore.exceptions.ClientError as err:
        failure_message = f"Failed to copy snapshot {source_snapshot_identifier}"
        slack_alert = error_message(err, account, failure_message)
        if err.response["Error"]["Code"] in ['SnapshotQuotaExceeded']:
            handle_error(err, env, region, failure_message, slack_alert)
            sys.exit(1)
        elif err.response["Error"]["Code"] in ['DBSnapshotAlreadyExists']:
            logging.warning(
                f"{env} {region}: The snapshot with the name {target_snapshot_identifier} already exists in the system.")
        else:
            handle_error(err, env, region, failure_message, slack_alert)


def retention_policy(client, prefix, num):
    """
    Apply retention policy to the RDS snapshots.

    This function checks the number of snapshots for each RDS instance and deletes the oldest ones if there are more
    than `num_snapshots_to_keep` snapshots. Only the last `num_snapshots_to_keep` snapshots for each RDS instance will be kept in the account.

    Args:
        client (boto3.client): The AWS RDS client.
        prefix : Snapshot prefix.
        num (int): Number of snapshots to keep for each RDS instance.
    """
    logging.info(f"Production : Applying retention policy to snapshots with prefix: {prefix}")
    paginator = client.get_paginator('describe_db_instances')
    instances = []
    for page in paginator.paginate():
        instances.extend(page['DBInstances'])

    paginator = client.get_paginator('describe_db_snapshots')
    snapshots = []
    for page in paginator.paginate(SnapshotType='manual'):
        snapshots.extend(page['DBSnapshots'])

    for instance in instances:
        result = list_resource_by_tags(instance, 'DBInstance', client)
        if result:
            resource_arn, resource_name = result
            instance_snapshots = [snapshot for snapshot in snapshots
                                  if snapshot['DBInstanceIdentifier'] == resource_name
                                  and snapshot['DBSnapshotIdentifier'].startswith(prefix)
                                  and snapshot['Status'] == 'available']
            logging.info(f"Number of instance snapshots: {len(instance_snapshots)}")
            if len(instance_snapshots) > 0:
                logging.info(
                    f"Production : Found {len(instance_snapshots)} snapshots for instance: "
                    f"{instance['DBInstanceIdentifier']}")
            if len(instance_snapshots) >= 2:
                sorted_snapshots = sorted(instance_snapshots, key=lambda x: x['SnapshotCreateTime'])
                for snapshot in sorted_snapshots[:-num]:
                    logging.info(
                        f"Production : Deleting snapshot copy: {snapshot['DBSnapshotIdentifier']}")
                    if not dry_run:
                        client.delete_db_snapshot(DBSnapshotIdentifier=snapshot['DBSnapshotIdentifier'])