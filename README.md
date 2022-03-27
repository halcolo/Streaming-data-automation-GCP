# Data streaming automation with GCP

* **category**    Data processing
* **author**      Juan Diego Alfonso <jalfons.ocampo@gmail.com>

## This repository shows how to create a new process in 

## Steps

This steps shows how to set the project is important setting a database in cloud firestore before star this process to create logs of the process.




### Create all variables and create buckets

``` sh
REGION=us-west2 
GCP_PROJECT=[VAR]
BUCKET_FILES_DATASETS=[VAR]
BUCKET_FUNCTION_STAGE=[VAR]
BUCKET_FILES_ERROR=[VAR]
BUCKET_FILES_SUCCESS=[VAR]
```

creating buckets.

``` sh
gsutil mb -c regional -l ${REGION} gs://${BUCKET_FILES_DATASETS}
gsutil mb -c regional -l ${REGION} gs://${BUCKET_FUNCTION_STAGE}
gsutil mb -c regional -l ${REGION} gs://${BUCKET_FILES_ERROR}
gsutil mb -c regional -l ${REGION} gs://${BUCKET_FILES_SUCCESS}
```

### Create Bigquery dataset and tables

``` sh
$ bq mk ${BUCKET_FILES_DATASETS}.hiring_employees schema_hiring_employees.json
$ bq mk ${BUCKET_FILES_DATASETS}.departments schema_departments.json
$ bq mk ${BUCKET_FILES_DATASETS}.jobs schema_jobs.json
```
### Creating batch function

Following function is a batch process to populate the database with the files that are uploaded to the bucket setted in `BUCKET_FILES_DATASETS`
Be sure the name of each file is the name of the table you want to populate and the csv have a header with the same columns of the shcema in the database, otherwise the process will fail.

``` sh
gcloud functions deploy batch_load --region=${REGION} \
    --source=./functions/batch_load_function --runtime=python39 \
    --stage-bucket=${BUCKET_FUNCTION_STAGE} \
    --trigger-bucket=${BUCKET_FILES_DATASETS} \
    --set-env-vars GCP_PROJECT=${GCP_PROJECT}
```

| Requires| Version|
| ------ | ------ |
| Python| `Python 3.9` |
| google-cloud-firestore| `0.30.1` |
| google-cloud-pubsub| `0.34.0` |
| google-cloud-storage| `1.13.1` |
| google-cloud-bigquery| `1.8.1` |
| google-cloud-core| `0.29.1` |
| pytz| `2018.7` |
| gcsfs| `2022.2.0` |
| fsspec| `2022.2.0` |
| pandas| `1.3.5` |



### Provisioning error and success PubSubTopics

This PubSub topics will trigger other functions to move the file from the principal bucket to other buckets and analise if fail or are success. 
``` sh
BATCH_ERROR_TOPIC=batch_error_topic
BATCH_SUCCESS_TOPIC=batch_success_topic

gcloud pubsub topics create ${BATCH_ERROR_TOPIC}
gcloud pubsub topics create ${BATCH_SUCCESS_TOPIC}
```

Following functions will be used to move the file it depend's the topic was triggered as result of the batch function.

``` sh
gcloud functions deploy batch_error --region=${REGION} \
    --source=./functions/move_file \
    --entry-point=move_file --runtime=python39 \
    --stage-bucket=${BUCKET_FUNCTION_STAGE} \
    --trigger-topic=${BATCH_ERROR_TOPIC} \
    --set-env-vars SOURCE_BUCKET=${BUCKET_FILES_DATASETS},DESTINATION_BUCKET=${BUCKET_FILES_ERROR}
```

```sh
gcloud functions deploy batch_success --region=${REGION} \
    --source=./functions/move_file \
    --entry-point=move_file --runtime=python39 \
    --stage-bucket=${BUCKET_FUNCTION_STAGE} \
    --trigger-topic=${BATCH_SUCCESS_TOPIC} \
    --set-env-vars SOURCE_BUCKET=${BUCKET_FILES_DATASETS},DESTINATION_BUCKET=${BUCKET_FILES_SUCCESS}
```

| Requires| Version|
| ------ | ------ |
| Python| `Python 3.9` |
| google-cloud-storage| `1.13.1` |


```
gcloud functions deploy streaming_load --region=${REGION} \
    --source=./functions/streaming_api_function --runtime=python39 \
    --entry-point=streaming_request_http \
    --stage-bucket=${BUCKET_FUNCTION_STAGE} \
    --trigger-http \
    --set-env-vars GCP_PROJECT=${GCP_PROJECT}
```