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
from datetime import datetime
from typing import List, Dict

from .utils.aws_utils import get_rds_client, get_regions
from .utils.slack_notifier import notify_backup_status
from .region_manager import RegionManager
from .snapshot_validator import SnapshotValidator


def get_dbs(client):
    """
    Getting a list of existing databases.
    """
    logging.info(f"{global_vars.env} {global_vars.region}: Listing existing DB Instances")
    try:
        dbs = client.describe_db_instances()
        db_with_tags = []
        for db in dbs['DBInstances']:
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


class RDSBackupManager:
    def __init__(self, source_region: str, target_regions: List[str]):
        self.source_region = source_region
        self.target_regions = target_regions
        self.rds_client = get_rds_client(source_region)
        self.region_manager = RegionManager(source_region, target_regions)
        self.validator = SnapshotValidator()
        self.logger = logging.getLogger(__name__)

    def create_snapshot(self, db_instance_id: str) -> Dict:
        """Create a snapshot of the specified RDS instance"""
        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M')
        snapshot_id = f"{db_instance_id}-backup-{timestamp}"
        
        try:
            response = self.rds_client.create_db_snapshot(
                DBSnapshotIdentifier=snapshot_id,
                DBInstanceIdentifier=db_instance_id
            )
            self.logger.info(f"Created snapshot {snapshot_id} for {db_instance_id}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to create snapshot for {db_instance_id}: {str(e)}")
            notify_backup_status(f"❌ Failed to create snapshot for {db_instance_id}", error=True)
            raise

    def replicate_snapshot(self, snapshot_id: str) -> List[Dict]:
        """Replicate snapshot to target regions"""
        results = []
        for region in self.target_regions:
            try:
                result = self.region_manager.copy_snapshot_to_region(snapshot_id, region)
                results.append(result)
                self.logger.info(f"Replicated snapshot {snapshot_id} to {region}")
            except Exception as e:
                self.logger.error(f"Failed to replicate snapshot to {region}: {str(e)}")
                notify_backup_status(f"❌ Failed to replicate snapshot to {region}", error=True)
                raise
        return results

    def run_backup(self, db_instance_id: str) -> None:
        """Execute full backup process including validation"""
        try:
            # Create snapshot
            snapshot = self.create_snapshot(db_instance_id)
            snapshot_id = snapshot['DBSnapshot']['DBSnapshotIdentifier']
            
            # Wait for snapshot to be available
            self.validator.wait_for_snapshot_available(self.rds_client, snapshot_id)
            
            # Replicate to target regions
            replicated_snapshots = self.replicate_snapshot(snapshot_id)
            
            # Validate replicated snapshots
            for snapshot in replicated_snapshots:
                region = snapshot['DestinationRegion']
                copied_snapshot_id = snapshot['DBSnapshot']['DBSnapshotIdentifier']
                region_client = get_rds_client(region)
                self.validator.wait_for_snapshot_available(region_client, copied_snapshot_id)
            
            notify_backup_status(f"✅ Successfully backed up {db_instance_id} to all regions")
            
        except Exception as e:
            self.logger.error(f"Backup process failed: {str(e)}")
            notify_backup_status(f"❌ Backup process failed for {db_instance_id}", error=True)
            raise


if __name__ == "__main__":
    main()
