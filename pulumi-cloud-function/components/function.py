from .shared_imports import (
    Optional,
    ComponentResource,
    Input,
    Output,
    ResourceOptions,
    cloudfunctionsv2,
)


class FunctionComponent(ComponentResource):
    """
    Create a Cloud Function.
    Args:
        name: str, the name of the function
        location: str, the location of the function
        description: Optional[str], the description of the function
        build_config_runtime: Optional[str], the runtime of the function
        build_config_entry_point: Optional[str], the entry point of the function
        build_config_storage_bucket: Optional[Output[str]], the storage bucket of the function
        build_config_storage_object: Optional[Output[str]], the storage object of the function
        service_account_email: Optional[Input[str]], the service account email of the function
        pubsub_topic: Optional[Input[str]], the pubsub topic of the function
        timeout_seconds: Optional[int], the timeout seconds of the function
        opts: Optional[ResourceOptions], the resource options
    """

    def __init__(
        self,
        name: str,
        location: str,
        description: Optional[str] = None,
        build_config_runtime: Optional[str] = None,
        build_config_entry_point: Optional[str] = None,
        build_config_storage_bucket: Optional[Output[str]] = None,
        build_config_storage_object: Optional[Output[str]] = None,
        service_account_email: Optional[Input[str]] = None,
        pubsub_topic: Optional[Input[str]] = None,
        timeout_seconds: Optional[int] = None,
        opts: Optional[ResourceOptions] = None,
    ):
        super().__init__("custom:resource:FunctionComponent", name, {}, opts)

        # Create event trigger if pubsub_topic is provided

        # Create the function
        self.function = cloudfunctionsv2.Function(
            resource_name=name,
            name=name,
            location=location,
            description=description,
            build_config=cloudfunctionsv2.FunctionBuildConfigArgs(
                runtime=build_config_runtime,
                entry_point=build_config_entry_point,
                source=cloudfunctionsv2.FunctionBuildConfigSourceArgs(
                    storage_source=cloudfunctionsv2.FunctionBuildConfigSourceStorageSourceArgs(
                        bucket=build_config_storage_bucket,
                        object=build_config_storage_object,
                    ),
                ),
                environment_variables={"GOOGLE_FUNCTION_SOURCE": "backup-dr.py"},
            ),
            service_config=cloudfunctionsv2.FunctionServiceConfigArgs(
                service_account_email=service_account_email,
                timeout_seconds=timeout_seconds,
            ),
            event_trigger=cloudfunctionsv2.FunctionEventTriggerArgs(
                event_type="google.cloud.pubsub.topic.v1.messagePublished",
                pubsub_topic=pubsub_topic,
                service_account_email=service_account_email,
            ),
            opts=ResourceOptions(parent=self),
        )

        # Register outputs
        self.register_outputs(
            {"function_name": self.function.name, "function_id": self.function.id}
        )
