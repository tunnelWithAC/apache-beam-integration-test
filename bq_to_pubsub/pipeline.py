import argparse
import json
import logging

import apache_beam as beam
from apache_beam import DoFn, PTransform
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions

from transforms.parse import Parse
from transforms.batch_write_to_pubsub import PubsubWriter, BatchWriteToPubsub


def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input_query')
  # parser.add_argument('--bigquery_table', required=True)
  # parser.add_argument('--input_subscription', required=True)
  # parser.add_argument('--output_topic', required=True)

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions()

  input_query = known_args.input_query

  with beam.Pipeline(options=pipeline_options) as p:
    # Read from PubSub into a PCollection.
    lines = (p | 'QueryTableStdSQL' >> beam.io.ReadFromBigQuery(
                query=input_query,
                use_standard_sql=True)
            )

    (lines | 'Convert to ByteString' >>BatchWriteToPubsub(topic='projects/conall-sandbox/topics/test-topic'))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
