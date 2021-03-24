
"""
A basic PubSub to PubSub/BigQuery workflow.

Run locally
python porter.py \
    --input_subscription "projects/conall-sandbox/subscriptions/wordcount-input-sub" \
    --output_topic "projects/conall-sandbox/topics/wordcount-output" \
    --bigquery_output "conall-sandbox:test.pipeline"

Run on dataflow

REGION=europe-west1
BUCKET=word_count_example_test
PROJECT=conall-sandbox

python porter.py \
  --region $REGION \
  --input_subscription "projects/conall-sandbox/subscriptions/wordcount-input-sub" \
  --output_topic "projects/conall-sandbox/topics/wordcount-output" \
  --bigquery_output "conall-sandbox:test.pipeline" \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/ \
  --job_name my-new-dataflow \
  --enable-streaming-engine
"""

# pytype: skip-file

# from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
#   parser.add_argument('--bigquery_output', required=True)
  parser.add_argument('--output_topic', required=True)
  parser.add_argument('--input_subscription', required=True)

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(StandardOptions).streaming = True
  with beam.Pipeline(options=pipeline_options) as p:
    # Read from PubSub into a PCollection.
    messages = (
        p |
        beam.io.ReadFromPubSub(subscription=known_args.input_subscription).
        with_output_types(bytes)
    )

    lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))

    def format_pubsub(msg):
        logging.info(f'Format PubSub: {msg}')
        return str(msg)

    output = (
        lines
        | 'format' >> beam.Map(format_pubsub)
        | 'encode' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))

    output | beam.io.WriteToPubSub(known_args.output_topic)

    # def format_bq(msg):
    #     m = { 'text' : msg }
    #     logging.info(f'Format BQ: {m}')
    #     return m

    # (lines
    #     | 'BQ Format' >> beam.Map(format_bq)
    #     | 'Write to BQ' >> beam.io.WriteToBigQuery(
    #                         table=known_args.bigquery_output, 
    #                         insert_retry_strategy='NEVER'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()