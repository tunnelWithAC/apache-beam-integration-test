import uuid

from google.cloud import pubsub


class PubSubSetupClient():

    def __init__(self, project):
        self.project = project
        self.pub_client = pubsub.PublisherClient()
        self.sub_client = pubsub.SubscriberClient()
        self.uuid = str(uuid.uuid4())

    def create_topic(self, topic):
        return self.pub_client.create_topic(
            self.pub_client.topic_path(self.project, topic + self.uuid))

    def create_subscription(self, topic, subscription):
        return self.sub_client.create_subscription(
            self.sub_client.subscription_path(self.project, subscription + self.uuid),
            topic.name)

