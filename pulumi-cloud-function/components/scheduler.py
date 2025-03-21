from .shared_imports import (
    List,
    Optional,
    ComponentResource,
    Input,
    Resource,
    ResourceOptions,
    cloudscheduler,
    std
)


class Scheduler(ComponentResource):
    """
    Create a Cloud Scheduler job.
    Args:
        name: str, the name of the scheduler job
        description: str, the description of the scheduler job
        schedule: str, the schedule of the scheduler job
        time_zone: str, the time zone of the scheduler job
    """
    def __init__(
        self,
        name: str,
        description: str,
        schedule: str,
        time_zone: str,
        pubsub_topic: Input[str],
        message_body: str,
        region: Optional[str] = None,
        depends_on: Optional[List[Input[Resource]]] = None,
        opts: ResourceOptions = ResourceOptions(parent=None, depends_on=None),
    ):
        super().__init__('custom:resource:Scheduler', name, {}, opts)

        self.job = cloudscheduler.Job(
            f"{name}-job",
            name=name,
            time_zone=time_zone,
            description=description,
            schedule=schedule,
            region=region,
            pubsub_target={
                "topic_name": pubsub_topic,
                "data": std.base64encode(input=message_body).result,
            },
            opts=ResourceOptions(parent=self, depends_on=depends_on),
        )

        self.register_outputs(
            {
                "job_id": self.job.id,
                "job_state": self.job.state,
            }
        )
