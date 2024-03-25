import boto3

class AWSUtils:
    def __init__(self):
        self.rds = boto3.client("rds")
        self.ec2 = boto3.client("ec2")

    def list_instances(self):
        return self.rds.describe_db_instances()

    def create_snapshot(self, instance_id):
        return self.rds.create_db_snapshot()

