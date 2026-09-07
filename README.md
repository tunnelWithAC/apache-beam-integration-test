## Apache Beam Integration Test Example

This repo contains example code for an Apache Beam pipeline that reads from Google Cloud PubSub and writes to another PubSub topic as well as a BigQuery table.

There are also examples of unit tests in the `tests/transforms` directory and an integration test in the `tests/pubsub_it_test.py` file.

This pipeline is taken from when I first learned how to write Apache Beam integration tests back in January 2020 and was heavily driven by [this question on Stackoverflow](https://stackoverflow.com/questions/66695171/how-do-i-run-apache-beam-integration-tests).

THe `.github/workflows/pr_checks.yaml` file shows you how to run tests as part of a Github Action.



### Installation Steps

This project uses [uv](https://docs.astral.sh/uv/). Install it, then run:

```
uv sync
```

That creates `.venv`, installs Python 3.12 if it isn't already present, and installs
everything from the committed `uv.lock`. Prefix commands with `uv run` to use it.

Set environment variables
```
BUCKET=
BIGQUERY_DATASET=
BIGQUERY_TABLE=
INPUT_SUB=
OUTPUT_TOPIC=
PROJECT=
REGION=europe-west1
```

### Run pipelines

Run pipeline using DirectRunner

```
uv run python main.py \
  --bigquery_dataset="$BIGQUERY_DATASET" \
  --bigquery_table="$BIGQUERY_TABLE" \
  --input_subscription "projects/$PROJECT/subscriptions/$INPUT_SUB" \
  --output_topic "projects/$PROJECT/topics/$OUTPUT_TOPIC"
```

Run pipeline using DataflowRunner

```
uv run python main.py \
  --setup_file ./pyproject.toml \
  --region $REGION \
  --input_subscription "projects/$PROJECT/subscriptions/$INPUT_SUB" \
  --output_topic "projects/$PROJECT/topics/wordcount-output" \
  --runner DataflowRunner \
  --project $PROJECT \
  --temp_location gs://$BUCKET/tmp/ \
  --job_name my-new-dataflow-again \
  --enable-streaming-engine
```

### Run tests

Unit tests need no GCP access:

```
uv run pytest
```

Integration tests are marked `it` and deselected by default, since they need a live
GCP project. Opt into them with `-m it`.

Run integration test using TestDirectRunner

```
uv run pytest -m it --log-cli-level=INFO tests/pubsub_it_test.py \
  --test-pipeline-options="--runner=TestDirectRunner \
  --project=$PROJECT --region=europe-west1 \
  --staging_location=gs://$BUCKET/staging \
  --temp_location=gs://$BUCKET/temp \
  --setup_file ./pyproject.toml"
```

Run integration test using TestDataflowRunner
```
uv run pytest -m it --log-cli-level=INFO tests/pubsub_it_test.py \
  --test-pipeline-options="--runner=TestDataflowRunner \
  --project=$PROJECT --region=europe-west1 \
  --staging_location=gs://$BUCKET/staging \
  --temp_location=gs://$BUCKET/temp \
  --job_name=it-test-pipeline \
  --setup_file ./pyproject.toml"
```

## Testing Notes

### Integration Tests
[Integration tests](https://cloud.google.com/architecture/building-production-ready-data-pipelines-using-dataflow-developing-and-testing#integration_tests) verify the correct functioning of your entire pipeline.

#### How to write Transform Integration Tests
A transform integration test that assesses the integrated functionality of all the individual transforms that make up your data pipeline. 

You can think of transform integration tests as a unit test for your entire pipeline, excluding integration with external data sources and sinks. 

The Beam SDK provides methods to supply test data to your data pipeline and to verify the results of processing. 

The Direct Runner is used to run transform integration tests.

#### How to write System Integration Tests
A system integration test that assesses your data pipeline's integration with external data sources and sinks. 

For your pipeline to communicate with external systems, you need to configure your tests with appropriate credentials to access external services. 

Streaming pipelines also run indefinitely, so you need to decide when and how to terminate the running pipeline. 

By using the Direct Runner to run system integration tests, you quickly verify the integration between your pipeline and other systems without needing to submit a Dataflow job and waiting for it to finish.

To create an integration test you must:

##### Create temporarary input and output data sources sinks dedicated to each test run
This code snippet shows an example of how to create Pubsub Topics/ Subscriptions and a BigQuery table using Python packages

```
INPUT_TOPIC = 'wordcount-input-'
OUTPUT_TOPIC = 'wordcount-output-'
INPUT_SUB = 'wordcount-input-sub-'
OUTPUT_SUB = 'wordcount-output-sub-'
OUTPUT_DATASET = 'it_dataset'
OUTPUT_TABLE = 'pubsub'
WAIT_UNTIL_FINISH_DURATION = 6 * 60 * 1000

from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper, parse_table_schema_from_json
from google.cloud import pubsub
from utils.create_pubsub import PubSubSetupClient # custom Python package

self.pub_client = pubsub.PublisherClient()
self.pubsub_setup_client = PubSubSetupClient(project=self.project)

self.input_topic = self.pubsub_setup_client.create_topic(INPUT_TOPIC)
self.output_topic = self.pubsub_setup_client.create_topic(OUTPUT_TOPIC)

self.input_sub = self.pubsub_setup_client.create_subscription(self.input_topic, INPUT_SUB)
self.output_sub = self.pubsub_setup_client.create_subscription(self.output_topic, OUTPUT_SUB)

# Set up BigQuery tables
self.dataset_ref = utils.create_bq_dataset(self.project, OUTPUT_DATASET)
self.bq_wrapper = BigQueryWrapper()
table_schema = parse_table_schema_from_json(schemas.get_test_schema())

def _create_table(table_id, schema):
    return self.bq_wrapper.get_or_create_table(project_id=self.project, 
                                                dataset_id=self.dataset_ref.dataset_id,
                                                table_id=table_id,
                                                schema=schema,
                                                create_disposition='CREATE_IF_NEEDED',
                                                write_disposition='WRITE_APPEND')


self.table_ref = _create_table(OUTPUT_TABLE, table_schema)
```

##### Run your pipeline. Apache Beam provides a TestDirectRunner and TestDataflowRunner

```
from pubsub_to_bq import pipeline, schemas

...

self.test_pipeline = TestPipeline(is_integration_test=True)
extra_opts = {
    'bigquery_dataset': self.dataset_ref.dataset_id,
    'bigquery_table': OUTPUT_TABLE,
    'input_subscription': self.input_sub.name,
    'output_topic': self.output_topic.name,
    'wait_until_finish_duration': WAIT_UNTIL_FINISH_DURATION,
    'on_success_matcher':
        all_of(bigquery_streaming_verifier, state_verifier, pubsub_msg_verifier) # bigquery_verifier
}
pipeline.run(self.test_pipeline.get_full_options_as_args(**extra_opts))
```

In order to get the TestDataflowRunner working you need to use the following project structure. `pubsub_to_bq` should be replaced witha n appropriate directory name for your pipeline.

```
main.py
pyproject.toml
uv.lock
transforms/
tests/pubsub_it_test.py
pubsub_to_bq/
```

The integration test can be ran using PyTest and the following command
```
uv run pytest -m it --log-cli-level=INFO tests/pubsub_it_test.py \
  --test-pipeline-options="--runner=TestDataflowRunner \
  --project=$PROJECT --region=europe-west1 \
  --staging_location=gs://$BUCKET/staging \
  --temp_location=gs://$BUCKET/temp \
  --job_name=it-test-pipeline \
  --setup_file ./pyproject.toml"
```

##### You will need to inject mock data into your pipeline

##### Verify the output of the pipeline

Pipeline State Verifiers

```
state_verifier = PipelineStateMatcher(PipelineState.RUNNING)
```

Pubsub Message Matcher
```
expected_msg = [ 'conall_0 - 1608051184'.encode('utf-8') ]
pubsub_msg_verifier = PubSubMessageMatcher(self.project, self.output_sub.name, expected_msg, timeout=60 * 7) # in seconds
```

Bigquery Message Matcher
```
validation_query = f'SELECT text FROM `{self.project}.{self.dataset_ref.dataset_id}.{OUTPUT_TABLE}`'
expected_bq_msg = [('conall_0 - 1608051184',)] # make sure you put the expected result in a tuple with a trailing comma

# Fetch Bigquery data with given query, compare to the expected data.
# This matcher polls BigQuery until the no. of records in BigQuery is
# equal to the no. of records in expected data.
# Specifying a timeout is optional
bigquery_streaming_verifier = BigqueryFullResultStreamingMatcher(
    project=self.project,
    query=validation_query,
    data=expected_bq_msg,
    timeout=60 * 7)

```

##### Teardown temporary resources
Any resources that were created during the test, e.g. Pubsub Topics & Subscriptions, should be removed at the end of the tests.
```
from apache_beam.io.gcp.tests import utils
def _cleanup_pubsub(self):
    test_utils.cleanup_subscriptions(self.pubsub_setup_client.sub_client, [self.input_sub, self.output_sub])
    test_utils.cleanup_topics(self.pubsub_setup_client.pub_client, [self.input_topic, self.output_topic])

self.addCleanup(self._cleanup_pubsub)
self.addCleanup(utils.delete_bq_dataset, self.project, self.dataset_ref)
```

### Other

The bullet points below are all covered in the [Official Dataflow Documentation](https://cloud.google.com/architecture/building-production-ready-data-pipelines-using-dataflow-developing-and-testing#testing_your_pipeline)

* DoFn Unit Tests
* Composite Transform Unit Test
* Transform Integration test / Pipeline Unit Test
* System Integration Test (Using % of test dataset)
* End-to-end test (using full test dataset)