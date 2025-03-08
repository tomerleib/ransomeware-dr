from google.cloud import storage
from .utils.logger import setup_logger

class BackupValidator:
    def __init__(self):
        self.storage_client = storage.Client()
        self.logger = setup_logger()

    def validate_backup(self, bucket_name, backup_name):
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.get_blob(backup_name)
            
            if not blob:
                raise ValueError(f"Backup {backup_name} not found")
                
            metadata = blob.metadata or {}
            if "backup_status" not in metadata or metadata["backup_status"] != "complete":
                raise ValueError(f"Backup {backup_name} is incomplete")
                
            return True
        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            return False
