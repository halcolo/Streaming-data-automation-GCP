'''
This Function is triggered when a new file is uploaded to the bucket
'''

import json
import logging
import os
import traceback
from datetime import datetime

from google.cloud import bigquery, firestore
from google.cloud import pubsub_v1, storage
import pandas as pd
import pytz


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'batch_error_topic')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'batch_success_topic')
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
BQ = bigquery.Client()
DB = firestore.Client()


def batch_load(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    db_ref = DB.document('batch_files/%s' % (file_name))
    try:
        _insert_into_bigquery(bucket_name, file_name)
        _handle_success(db_ref)
    except Exception:
        _handle_error(db_ref)


def _insert_into_bigquery(bucket_name, file_name):
    ''' This process sed data from file to GBQ if exist table and match with schema '''
    path = 'gs://%s/%s' % (bucket_name, file_name)
    name = file_name.split('.')[0]
    table_name = name
    data_frame = pd.read_csv(path)
    row = json.loads(json.dumps(data_frame.to_dict('records')))
    table_id = '%s.%s.%s' % (PROJECT_ID, BQ_DATASET, table_name)
    errors = BQ.insert_rows_json(table=table_id,
                                 json_rows=row)

    if errors != []:
        raise BigQueryError(errors)


def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        'success': True,
        'when': _now()
    }
    db_ref.set(doc)
    PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error streaming file \'%s\'. Cause: %s' % (
        db_ref.id, traceback.format_exc())
    doc = {
        'success': False,
        'error_message': message,
        'when': _now()
    }
    db_ref.set(doc)
    PS.publish(ERROR_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.error(message)


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')


class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened'''
    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)
