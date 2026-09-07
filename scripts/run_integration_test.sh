#!/usr/bin/env bash
set -euo pipefail

PROJECT="${PROJECT:?PROJECT must be set}"
BUCKET="${BUCKET:?BUCKET must be set}"

uv run pytest -m it --log-cli-level=INFO tests/pubsub_it_test.py \
  --test-pipeline-options="--runner=TestDataflowRunner \
    --project=${PROJECT} --region=europe-west1 \
    --staging_location=gs://${BUCKET}/staging \
    --temp_location=gs://${BUCKET}/temp \
    --job_name=it-test-pipeline \
    --setup_file ./pyproject.toml"
