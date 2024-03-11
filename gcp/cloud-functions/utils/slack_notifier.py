from slack_sdk import WebClient

class SlackNotifier:
    def __init__(self, token):
        self.client = WebClient(token=token)

    def notify_backup_start(self, bucket, blob):
        self.client.chat_postMessage(
            channel="#gcp-backups",
            text=f"Starting backup for {bucket}/{blob}"
        )

    def notify_backup_complete(self, bucket, blob):
        self.client.chat_postMessage(
            channel="#gcp-backups",
            text=f"Backup completed for {bucket}/{blob}"
        )

    def notify_error(self, message):
        self.client.chat_postMessage(
            channel="#gcp-backups",
            text=f"Error: {message}"
        )
