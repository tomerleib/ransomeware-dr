"""A Google Cloud Python Pulumi program that creates a service account for a Cloud Function."""

from components.shared_imports import export, os
from components.sa import ServiceAccount
from components.config import get_config

# Get GCP configuration (namespaced)
gcp_config = get_config(namespace="gcp", required_fields=["region"])
gcp_project = os.environ.get("GOOGLE_PROJECT")

# Get service account configuration (non-namespaced)
sa_config = get_config(required_fields=["name", "description", "roles", "display_name"])

# Create service account
function_sa = ServiceAccount(
    sa_config["name"],
    description=sa_config["description"],
    roles=sa_config["roles"],
    display_name=sa_config["display_name"],
    gcp_project=gcp_project
)

# Export values
export("sa_name", sa_config["name"])
export("sa_email", function_sa.sa.email)
export("region", gcp_config["region"])
export("project", gcp_project)
export("project_id", function_sa.sa.project)
