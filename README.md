# NowPlaying

Example project demonstrating the processing and analysis of unstructured tweet like data in batch and real-time using Cloud DataFlow and BigQuery.

## Building

Authenticate with the GCP project via `gcloud auth application-default login`

### Set required environment variables
```
export REGION=""
export GCP_PROJECT=""
export BQ_DATASET=""
export GCP_BUCKET=""
export TEMPLATE_IMAGE="gcr.io/$GCP_PROJECT/dataflow-twitter:latest"
export TEMPLATE_PATH="gs://$GCP_BUCKET/dataflow-templates/dataflow-twitter.json"
```

### Build flex template docker image
```
gcloud builds submit --tag $TEMPLATE_IMAGE .
```

### Build the flex template
```
gcloud dataflow flex-template build $TEMPLATE_PATH \
       --image "$TEMPLATE_IMAGE" \
       --sdk-language "PYTHON"
```


### Running a build with cloudbuild.yaml config
```
gcloud builds submit --config=cloudbuild.yaml --project=$GCP_PROJECT --substitutions=_TEMPLATE_IMAGE="gcr.io/$GCP_PROJECT/dataflow-twitter:latest",_GCP_BUCKET="your-project-bucket-name"
```

## Running the flex template on Dataflow
```
gcloud dataflow flex-template run "twitter-beam-`date +%Y%m%d-%H%M%S`" \
    --template-file-gcs-location "$TEMPLATE_PATH" \
    --parameters input="gs://$GCP_BUCKET/input/logs.txt" \
    --parameters output="gs://$GCP_BUCKET/output/rejects.txt" \
    --parameters bqproject=$GCP_PROJECT \
    --parameters bqdataset=$BQ_DATASET \
    --parameters temp_location="gs://$GCP_BUCKET/tmp/" \
    --region "$REGION" \
    --project $GCP_PROJECT
```

## Contributing

See ["CONTRIBUTING.md"](docs/contributing.md) for details.

## License

Apache 2.0; see ["LICENSE"](LICENSE) for details.

## Disclaimer

This project is not an official Google project. It is not supported by
Google and Google specifically disclaims all warranties as to its quality,
merchantability, or fitness for a particular purpose.


