import logging

from bq_to_pubsub import pipeline

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  pipeline.run()

