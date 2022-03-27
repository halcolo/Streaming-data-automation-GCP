import os
import json
import functions_framework
from datetime import datetime
import pytz
import traceback
from google.cloud import bigquery, firestore
from google.cloud import pubsub_v1
from flask import abort


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = 'test_globant'
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'batch_error_topic')
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, 'batch_success_topic')
DB = firestore.Client()
BQ = bigquery.Client()
PS = pubsub_v1.PublisherClient()


@functions_framework.http
def streaming_request_http(request):
    if request.method == 'GET':
        db_ref = DB.document('streaming_process/process_%s' % (_now()))
        try:
            request_json = request.get_json(silent=True)
            request_args = request.args

            if request_json and 'data' in request_json and 'table_name' in request_json:
                table_name = request_json['table_name']
                data = request_json['data']
                _insert_into_bigquery(table_name, data)
            elif request_args and 'data' in request_args and 'table_name' in request_args:
                table_name = request_args['table_name']
                data = request_args['data']
                _insert_into_bigquery(table_name, data)
            return _handle_success(db_ref)
        except Exception:
            return _handle_error(db_ref)
    elif request.method == 'PUT':
        return abort(403)
    else:
        return abort(405)


def _insert_into_bigquery(table_name, data):
    row = json.loads(json.dumps(data))
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
    return message


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
    return message


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d_%H:%M:%S_%Z')


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
