"""A Google Cloud Python Pulumi program"""

from components.shared_imports import pulumi, Config, os
from components.sa import ServiceAccount

gcp_config = Config("gcp")
project = os.environ.get("GOOGLE_PROJECT")
region = gcp_config.require("region")
sa_config = Config("serviceaccount")
sa_name = sa_config.require("name")
sa_description = sa_config.require("description")
sa_roles = sa_config.require_object("roles")
sa_display_name = sa_config.require("display_name")

function_sa = ServiceAccount(
    sa_name,
    description=sa_description,
    roles=sa_roles,
    display_name=sa_display_name,
    project=project,
    region=region,
)


pulumi.export("function_sa_email", function_sa.sa.email)
