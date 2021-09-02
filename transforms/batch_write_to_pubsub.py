import logging

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage

from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.types import (
    BatchSettings,
    LimitExceededBehavior,
    PublishFlowControl,
    PublisherOptions,
)


class BatchWriteToPubsub(beam.PTransform):

    def __init__(self, topic):
        self.topic = topic

    def expand(self, pcoll):
        import json
        import logging

        (pcoll | 'Convert to ByteString' >> beam.Map(lambda x: json.dumps(x).encode('utf-8')).with_output_types(bytes)
               | 'Convert to Pubsub Message' >> beam.Map(lambda x: PubsubMessage(x, {}))
               | "PublishRowsToPubSub" >> beam.ParDo(PubsubWriter(topic=self.topic))
         )


class PublishClient(PublisherClient):
    """
    You have to override __reduce__ to make PublisherClient pickleable 😡 😤 🤬

    Props to 'Ankur' and 'Benjamin' on SO for figuring this part out; god knows
    I would not have...
    """

    def __reduce__(self):
        return self.__class__, (self.batch_settings, self.publisher_options)


class PubsubWriter(beam.DoFn):
    """
    beam.io.gcp.pubsub does not yet support batch operations, so
    we do this the hard way.  it's not as performant as the native
    pubsubio but it does the job.
    """

    def __init__(self, topic: str):
        self.topic = topic
        self.window = beam.window.GlobalWindow()
        self.count = 0

        batch_settings = BatchSettings(
            max_bytes=1e6,  # 1MB
            # by default it is 10 ms, should be less than timeout used in future.result() to avoid timeout
            max_latency=1,
        )

        publisher_options = PublisherOptions(
            enable_message_ordering=False,
            # better to be slow than to drop messages during a recovery...
            flow_control=PublishFlowControl(limit_exceeded_behavior=LimitExceededBehavior.BLOCK),
        )

        self.publisher = PublishClient(batch_settings, publisher_options)

    def start_bundle(self):
        self.futures = []

    def process(self, element: PubsubMessage, window=beam.DoFn.WindowParam):
        self.window = window
        self.futures.append(
            self.publisher.publish(
                topic=self.topic,
                data=element.data,
                **element.attributes,
            )
        )

    def finish_bundle(self):
        """Iterate over the list of async publish results and block
        until all of them have either succeeded or timed out.  Yield
        a WindowedValue of the success/fail counts."""

        results = []
        self.count = self.count + len(self.futures)
        for fut in self.futures:
            try:
                # future.result() blocks until success or timeout;
                # we've set a max_latency of 60s upstairs in BatchSettings,
                # so we should never spend much time waiting here.
                results.append(fut.result(timeout=60))
            except Exception as ex:
                results.append(ex)

        res_count = {"success": 0}
        for res in results:
            if isinstance(res, str):
                res_count["success"] += 1
            else:
                # if it's not a string, it's an exception
                msg = str(res)
                if msg not in res_count:
                    res_count[msg] = 1
                else:
                    res_count[msg] += 1

        logging.info(f"Pubsub publish results: {res_count}")

        yield beam.utils.windowed_value.WindowedValue(
            value=res_count,
            timestamp=0,
            windows=[self.window],
        )

    def teardown(self):
        logging.info(f"Published {self.count} messages")

