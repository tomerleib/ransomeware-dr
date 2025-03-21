"""A Google Cloud Python Pulumi program that creates a service account for a Cloud Function."""

from components.shared_imports import export, os
from components.sa import ServiceAccount
from components.config import get_config
from components.pubsub import PubSubTopic

# Get GCP configuration (namespaced)
gcp_config = get_config(namespace="gcp", required_fields=["region"])
gcp_project = os.environ.get("GOOGLE_PROJECT")

# Get service account configuration (non-namespaced)
sa_config = get_config(required_fields=["name", "description", "roles", "display_name"], config_key="serviceaccount")

# Get pubsub configuration (non-namespaced)
pubsub_config = get_config(config_key="pubsub", required_fields=["name", "message_retention_duration"])

# Create service account
function_sa = ServiceAccount(
    sa_config["name"],
    description=sa_config["description"],
    roles=sa_config["roles"],
    display_name=sa_config["display_name"],
    gcp_project=gcp_project
)

# Create pubsub topic
pubsub_topic = PubSubTopic(
    pubsub_config["name"],
    message_retention_duration=pubsub_config["message_retention_duration"]
)

# Export values
export("sa_name", sa_config["name"])
export("sa_email", function_sa.sa.email)
export("pubsub_topic_name", pubsub_topic.topic.name)