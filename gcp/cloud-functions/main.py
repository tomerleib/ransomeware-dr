from .backup.backup_manager import BackupManager

def backup_handler(event, context):
    """Cloud Function entry point"""
    manager = BackupManager()
    
    bucket = event["bucket"]
    name = event["name"]
    
    return manager.create_backup(bucket, name)
