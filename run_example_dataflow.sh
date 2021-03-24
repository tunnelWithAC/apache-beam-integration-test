REGION=europe-west1
BUCKET=word_count_example_test
PROJECT=conall-sandbox

python -m apache_beam.examples.wordcount \
  --region $REGION \
  --input gs://dataflow-samples/shakespeare/kinglear.txt \
  --output gs://$BUCKET/results/outputs \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/