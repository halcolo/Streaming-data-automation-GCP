'''
    This function create 1 or multiple registers from a table
    GET:
        Send json data to GBQ
'''
import os
import json
import pytz
import ast
import traceback
import functions_framework

from datetime import datetime
from google.cloud import bigquery, firestore
from google.cloud import pubsub_v1
from flask import abort
from flask import make_response, jsonify


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
DB = firestore.Client()
BQ = bigquery.Client()

@functions_framework.http
def streaming_request_http(request):
    if request.method == 'POST':
        db_ref = DB.document('streaming_process/process_%s' % (_now()))
        try:
            request_json = request.get_json(silent=True)
            request_args = request.args

            if request_json and 'data' in request_json and 'table_name' in request_json:
                table_name = request_json['table_name']
                data = request_json['data']
            elif request_args and 'data' in request_args and 'table_name' in request_args:
                table_name = request_args['table_name']
                data = request_args['data']
            else:
                raise Exception('Incorrect request %s' % _now())
            
            if type(data) == str:
                data = ast.literal_eval(data)
            _insert_into_bigquery(table_name, data)
                
            return _handle_success(db_ref)

        except Exception:
            return _handle_error(db_ref)

    else:
        return abort(405)


def _insert_into_bigquery(table_name, data):
    data = json.dumps(data)
    row = json.loads(data)
    table_id = '%s.%s.%s' % (PROJECT_ID, BQ_DATASET, table_name)
    errors = BQ.insert_rows_json(table=table_id,    
                                 json_rows=row)

    if errors != []:
        raise BigQueryError(errors)


def _handle_success(db_ref):
    message = 'Data added to BigQuery at \'%s\'' % db_ref.id
    doc = {
        'success': True,
        'when': _now()
    }
    db_ref.set(doc)
    return _handle_response(message, 200)


def _handle_error(db_ref):
    message = 'Error streaming data at \'%s\'. Cause: %s' % (
        db_ref.id, traceback.format_exc())
    doc = {
        'success': False,
        'error_message': message,
        'when': _now()
    }
    db_ref.set(doc)
    return _handle_response(message, 400)


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d_%H:%M:%S_%Z')


def _handle_response(message, code):
    response = jsonify(dict(message=message, code=code))
    return make_response(response, code)

class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened'''

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return _handle_response(json.dumps(err), 409)