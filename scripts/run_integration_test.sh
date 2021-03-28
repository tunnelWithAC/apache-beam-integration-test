pytest --log-cli-level=INFO tests/pubsub_it_test.py --test-pipeline-options="--runner=TestDataflowRunner \
            --project=${{ env.PROJECT }} --region=europe-west1 \
            --staging_location=gs://${{ env.BUCKET }}/staging \
            --temp_location=gs://${{ env.BUCKET }}/temp \
            --job_name=it-test-pipeline \
            --setup_file ./setup.py"