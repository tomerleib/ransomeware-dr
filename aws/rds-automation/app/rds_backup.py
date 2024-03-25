from .utils.aws_utils import AWSUtils
from .utils.slack_notifier import SlackNotifier
from .utils.logger import setup_logger

class RDSBackup:
    def __init__(self):
        self.aws = AWSUtils()
        self.logger = setup_logger()
        self.notifier = SlackNotifier("token")

    def backup_instance(self, instance_id):
        try:
            self.notifier.notify_backup_start(instance_id)
            snapshot = self.aws.create_snapshot(instance_id)
            self.notifier.notify_backup_complete(instance_id)
            return snapshot
        except Exception as e:
            self.logger.error(f"Backup failed: {str(e)}")
