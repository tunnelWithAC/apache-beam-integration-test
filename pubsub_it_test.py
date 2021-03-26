from __future__ import absolute_import

import logging
import os
import time
import unittest
import uuid

from hamcrest.core.core.allof import all_of
from nose.plugins.attrib import attr

from apache_beam.io.gcp.tests import utils
# from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryMatcher
from apache_beam.io.gcp.tests.pubsub_matcher import PubSubMessageMatcher
from apache_beam.runners.runner import PipelineState
from apache_beam.testing import test_utils
from apache_beam.testing.pipeline_verifiers import PipelineStateMatcher
from apache_beam.testing.test_pipeline import TestPipeline

import pipeline


INPUT_TOPIC = 'wordcount-input-'
OUTPUT_TOPIC = 'wordcount-output-'
INPUT_SUB = 'wordcount-input-sub-'
OUTPUT_SUB = 'wordcount-output-sub-'

OUTPUT_DATASET = 'it_dataset'
OUTPUT_TABLE_SESSIONS = 'pubsub'

DEFAULT_INPUT_NUMBERS = 1
WAIT_UNTIL_FINISH_DURATION = 12 * 60 * 1000  # in milliseconds


class TestIT(unittest.TestCase):
    def setUp(self):
        self.test_pipeline = TestPipeline(is_integration_test=True)
        self.project = self.test_pipeline.get_option('project')
        self.uuid = str(uuid.uuid4())

        # Set up PubSub environment.
        from google.cloud import pubsub
        self.pub_client = pubsub.PublisherClient()
        self.input_topic = self.pub_client.create_topic(
            self.pub_client.topic_path(self.project, INPUT_TOPIC + self.uuid))
        self.output_topic = self.pub_client.create_topic(
            self.pub_client.topic_path(self.project, OUTPUT_TOPIC + self.uuid))

        self.sub_client = pubsub.SubscriberClient()
        self.input_sub = self.sub_client.create_subscription(
            self.sub_client.subscription_path(self.project, INPUT_SUB + self.uuid),
            self.input_topic.name)
        self.output_sub = self.sub_client.create_subscription(
            self.sub_client.subscription_path(self.project, OUTPUT_SUB + self.uuid),
            self.output_topic.name,
            ack_deadline_seconds=60)

        # Set up BigQuery tables
        self.dataset_ref = utils.create_bq_dataset(self.project, self.OUTPUT_DATASET)
    
    def _inject_numbers(self, topic, num_messages):
        """Inject numbers as test data to PubSub."""
        logging.info('Injecting %d numbers to topic %s', num_messages, topic.name)
        for n in range(num_messages):
            self.pub_client.publish(self.input_topic.name, str(n).encode('utf-8'))

    # def tearDown(self):
    #     test_utils.cleanup_subscriptions(self.sub_client, [self.input_sub, self.output_sub])
    #     test_utils.cleanup_topics(self.pub_client, [self.input_topic, self.output_topic])

    def _cleanup_pubsub(self):
        # test_utils.cleanup_subscriptions(self.sub_client, [self.input_sub])
        # test_utils.cleanup_topics(self.pub_client, [self.input_topic])
        test_utils.cleanup_subscriptions(self.sub_client, [self.input_sub, self.output_sub])
        test_utils.cleanup_topics(self.pub_client, [self.input_topic, self.output_topic])
  
    @attr('IT')
    def test_pubsub_pipe_it(self):
        # Build expected dataset.
        expected_msg = [('%d' % num).encode('utf-8') for num in range(DEFAULT_INPUT_NUMBERS)]

        # Set extra options to the pipeline for test purpose
        state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
        pubsub_msg_verifier = PubSubMessageMatcher(self.project, self.output_sub.name, expected_msg, timeout=400)
        extra_opts = {
            'input_subscription': self.input_sub.name,
            'output_topic': self.output_topic.name,
            'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION,
            'on_success_matcher': all_of(state_verifier, pubsub_msg_verifier)
        }

        # Generate input data and inject to PubSub.
        self._inject_numbers(self.input_topic, DEFAULT_INPUT_NUMBERS)

        # Get pipeline options from command argument: --test-pipeline-options,
        # and start pipeline job by calling pipeline main function.
        pipeline.run(self.test_pipeline.get_full_options_as_args(**extra_opts))

        # Cleanup PubSub
        self.addCleanup(self._cleanup_pubsub)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
