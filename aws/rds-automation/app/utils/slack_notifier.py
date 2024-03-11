from slack_sdk import WebClient

class SlackNotifier:
    def __init__(self, token):
        self.client = WebClient(token=token)

    def notify_backup_start(self, instance):
        self.client.chat_postMessage(channel="#backups", text=f"Starting backup for {instance}")

    def notify_backup_complete(self, instance):
        self.client.chat_postMessage(channel="#backups", text=f"Backup completed for {instance}")
