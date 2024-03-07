import boto3
from .utils.logger import setup_logger

class RegionManager:
    def __init__(self):
        self.logger = setup_logger()
        self.regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

    def copy_snapshot_to_region(self, snapshot_id, source_region, target_region):
        try:
            target_rds = boto3.client("rds", region_name=target_region)
            response = target_rds.copy_db_snapshot(
                SourceDBSnapshotIdentifier=f"arn:aws:rds:{source_region}:{snapshot_id}",
                TargetDBSnapshotIdentifier=f"{snapshot_id}-{target_region}",
                SourceRegion=source_region
            )
            return response
        except Exception as e:
            self.logger.error(f"Failed to copy to {target_region}: {str(e)}")
