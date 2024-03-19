from .utils.aws_utils import AWSUtils
from .utils.logger import setup_logger

class SnapshotValidator:
    def __init__(self):
        self.aws = AWSUtils()
        self.logger = setup_logger()

    def validate_snapshot(self, snapshot_id):
        try:
            snapshot = self.aws.rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
            status = snapshot["DBSnapshots"][0]["Status"]
            return status == "available"
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False
