import os
import logging
import sys
from datetime import datetime, timezone
from botocore.config import Config

max_workers = 100
connection_max_attempts = 200

role_name = os.getenv('ROLE_NAME')
src_account = os.getenv('SOURCE_ACCOUNT_ID')
region = os.getenv("REGION")
dst_account = os.getenv('DST_ACCOUNT_ID')
timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M')
log_level = os.getenv('LOG_LEVEL', 'INFO')
num_retention = int(os.getenv('NUM_RETENTION', 2))
env = os.getenv('ENVIRONMENT', '')
application = os.getenv('APPLICATION', '')
group = os.getenv('GROUP', '').lower()
wrk_type = os.getenv('WORKLOAD_TYPE','main').lower()
dry_run = os.getenv('DRY_RUN', 'False').lower() == 'true'
ba_keys = ['owner', 'group', 'Group']
rds_service_values = ['postgresql', 'central-postgresql']
client_config = Config(
    region_name=region,
    max_pool_connections=max_workers,
    retries={
        'max_attempts': connection_max_attempts,
        'mode': 'adaptive'
    }
)


def error_message(e, account, message):
    e_message = {
        "Application": application,
        "Error Code": e.response['Error']['Code'],
        "Error Message": message,
        "Account": account,
        "Environment": env,
        "Region": region,
        "Issue": e.response['Error']['Message']
    }

    return e_message


def validate(*variables):
    """
    Check the existence of environment variables.

    :param variables: List of environment variable names to check.
    :raises ValueError: If any of the specified environment variables is missing.
    """
    for var in variables:
        if var not in os.environ:
            logging.error(f"Environment variable '{var}' is not set. Please set it before running the script.")
            sys.exit(1)
