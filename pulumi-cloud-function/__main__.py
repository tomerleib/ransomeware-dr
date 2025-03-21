"""A Google Cloud Python Pulumi program that creates a service account for a Cloud Function."""

from components.shared_imports import export, os, Output
from components.sa import ServiceAccount
from components.config import get_config
from components.pubsub import PubSubTopic
from components.scheduler import Scheduler
from components.storage import StorageComponent

SOURCE_CODE_DIR = "../gcp/cloud-functions"
# Get GCP configuration (namespaced)
gcp_config = get_config(namespace="gcp", required_fields=["region"])
gcp_project = os.environ.get("GOOGLE_PROJECT")

# Get service account configuration (non-namespaced)
sa_config = get_config(
    required_fields=["name", "description", "roles", "display_name"],
    config_key="serviceaccount",
)

# Get pubsub configuration (non-namespaced)
pubsub_config = get_config(
    config_key="pubsub", required_fields=["name", "message_retention_duration"]
)

# Get scheduler configuration (non-namespaced)
scheduler_config = get_config(
    config_key="cloud-scheduler",
    required_fields=["name", "description", "schedule", "time_zone", "message_body"]
)

# Get storage configuration (non-namespaced)
storage_config = get_config(
    config_key="storage", required_fields=["name", "location"]
)

# Create service account
function_sa = ServiceAccount(
    sa_config["name"],
    description=sa_config["description"],
    roles=sa_config["roles"],
    display_name=sa_config["display_name"],
    gcp_project=gcp_project,
    ignore_existing=True
)

# Create pubsub topic
pubsub_topic = PubSubTopic(
    pubsub_config["name"],
    message_retention_duration=pubsub_config["message_retention_duration"]
)

# Create scheduler
scheduler = Scheduler(
    scheduler_config["name"],
    description=scheduler_config["description"],
    schedule=scheduler_config["schedule"],
    time_zone=scheduler_config["time_zone"],
    message_body=scheduler_config["message_body"],
    pubsub_topic=Output.all(gcp_project, pubsub_topic.topic.name).apply(
        lambda args: f"projects/{args[0]}/topics/{args[1]}"
    )
)

# Create storage bucket
storage = StorageComponent(
    storage_config["name"],
    location=storage_config["location"],
    object_path=SOURCE_CODE_DIR
)

# Export values
export("sa_name", sa_config["name"])
export("sa_email", function_sa.sa.email)
export("pubsub_topic_name", pubsub_topic.topic.name)
