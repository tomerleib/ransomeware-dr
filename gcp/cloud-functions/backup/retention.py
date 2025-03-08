from datetime import datetime, timedelta
from .utils.gcp_utils import GCPUtils

class RetentionManager:
    def __init__(self):
        self.gcp = GCPUtils()

    def cleanup_old_backups(self, bucket_name, retention_days=30):
        bucket = self.gcp.storage_client.bucket(bucket_name)
        cutoff = datetime.now() - timedelta(days=retention_days)
        
        for blob in bucket.list_blobs(prefix="backup-"):
            if blob.time_created < cutoff:
                blob.delete()
