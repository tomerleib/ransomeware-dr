from google.cloud import storage

class GCPUtils:
    def __init__(self):
        self.storage_client = storage.Client()

    def list_buckets(self):
        return list(self.storage_client.list_buckets())

    def create_backup(self, bucket_name, source_blob, destination_blob):
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob)
        return blob.copy_to(bucket.blob(destination_blob))
