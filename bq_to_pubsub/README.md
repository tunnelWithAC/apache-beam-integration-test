### BigQuery to PubSub Batch Dataflow

This pipeline reads from BigQuery and writes all elements to a PubSub topic.

```bash
set -f
export BQ_QUERY='SELECT * FROM `clouddataflow-readonly.samples.weather_stations` LIMIT 10'

python bq_to_pubsub_main.py  \
    --project conall-sandbox \
    --temp_location=gs://dataflow-test-sandbox/bq_to_pubsub \
    --input_query="${BQ_QUERY}"
set +f
```