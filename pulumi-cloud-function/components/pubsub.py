from .shared_imports import (
    Optional,
    ComponentResource,
    ResourceOptions,
    pubsub
)

class PubSubTopic(ComponentResource):
    """
    Create a Pub/Sub topic.
    Args:
        name: str, the name of the topic
        opts: Optional[ResourceOptions], the options for the topic
        message_retention_duration: Optional[str], the message retention duration
        service_account_email: Optional[str], the service account email
    """
    def __init__(self, 
                 name: str, 
                 opts: Optional[ResourceOptions] = None,
                 message_retention_duration: Optional[str] = None):
        super().__init__('custom:resource:PubSubTopic', name, {}, opts)
        self.name = name

        self.topic = pubsub.Topic(
            self.name,
            name=self.name,
            message_retention_duration=message_retention_duration,
            opts=ResourceOptions(parent=self)
        )

        # Register useful outputs
        self.register_outputs({
            'topic_name': self.topic.name,
            'topic_id': self.topic.id
        })

    def get_topic(self) -> pubsub.Topic:
        """Returns the Pub/Sub topic resource"""
        return self.topic