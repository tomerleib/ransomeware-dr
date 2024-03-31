from datetime import datetime
from .utils.gcp_utils import GCPUtils
from .utils.slack_notifier import SlackNotifier
from .utils.logger import setup_logger

class BackupManager:
    def __init__(self):
        self.gcp = GCPUtils()
        self.logger = setup_logger()
        self.notifier = SlackNotifier("token")

    def create_backup(self, bucket_name, blob_name):
        try:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            destination = f"backup-{timestamp}-{blob_name}"
            
            self.notifier.notify_backup_start(bucket_name, blob_name)
            backup = self.gcp.create_backup(bucket_name, blob_name, destination)
            self.notifier.notify_backup_complete(bucket_name, destination)
            
            return backup
        except Exception as e:
            error_msg = f"Backup failed for {bucket_name}/{blob_name}: {str(e)}"
            self.logger.error(error_msg)
            self.notifier.notify_error(error_msg)
            raise
