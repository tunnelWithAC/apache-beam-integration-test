from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions

from transforms.parse import Parse


def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--bigquery_dataset', required=True, default='porter')
  parser.add_argument('--bigquery_table', required=True)
  parser.add_argument('--input_subscription', required=True)
  parser.add_argument('--output_topic', required=True)

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(StandardOptions).streaming = True

  with beam.Pipeline(options=pipeline_options) as p:
    # Read from PubSub into a PCollection.
    lines = (p 
      | beam.io.ReadFromPubSub(subscription=known_args.input_subscription, 
                               timestamp_attribute='timestamp'
                              ).with_output_types(bytes)
      | Parse()
    )

    output = (
        lines
        | 'encode' >> beam.Map(lambda x: x.encode('utf-8')).with_output_types(bytes))

    output | beam.io.WriteToPubSub(known_args.output_topic)

    def format_bq(msg):
        m = { 'text' : msg }
        logging.info(f'Format BQ: {m}')
        return m

    (lines
        | 'BQ Format' >> beam.Map(format_bq)
        | 'Write to BQ' >> beam.io.WriteToBigQuery(
                            table=known_args.bigquery_table,
                            dataset=known_args.bigquery_dataset, 
                            insert_retry_strategy='NEVER'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
