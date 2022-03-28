'''
    This function create an AVRO backup to GCS and restore it from GCS
    GET:
        Restores Avro file from GCS backup 
    POST:
        Create backcup file to GCS
'''

import os
import pytz
import traceback
import functions_framework

from datetime import datetime
from google.cloud import bigquery, firestore, storage
from flask import abort
from flask import make_response, jsonify


PROJECT_ID = os.getenv('GCP_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
LOCATION = 'US'
DB = firestore.Client()
BQ = bigquery.Client(PROJECT_ID)
CS = storage.Client()


@functions_framework.http
def backup_request_http(request):
    if request.method == 'GET':
        db_ref = DB.document('backup_process/backup_%s' % (_now()))
        try:
            request_json = request.get_json(silent=True)
            request_args = request.args

            if request_json and 'table_name' in request_json and 'bucket_name' in request_json:
                table_name = request_json['table_name']
                bucket_name = request_json['bucket_name']
            elif request_args and 'table_name' in request_args and 'bucket_name' in request_args:
                table_name = request_args['table_name']
                bucket_name = request_args['bucket_name']
            else:
                raise Exception('Incorrect request %s' % _now())

            _make_bigquery_backup(table_name, bucket_name)
            message = 'Table backd up successfully \'%s\'' % db_ref.id
            return _handle_success(db_ref, message)

        except Exception:
            message = 'Table backup error  \'%s\'. Cause: %s' % (
                db_ref.id, traceback.format_exc())
            return _handle_error(db_ref, message)
        
    elif request.method == 'POST':
        db_ref = DB.document('restore_process/restored_%s' % (_now()))
        try:
            request_json = request.get_json(silent=True)
            request_args = request.args

            if request_json \
                and 'table_name' in request_json \
                and 'bucket_name' in request_json \
                and 'file_name' in request_json:
                table_name = request_json['table_name']
                file_name = request_json['file_name']
                bucket_name = request_json['bucket_name']
            elif request_args \
                and 'table_name' in request_args \
                    and 'bucket_name' in request_args \
                    and 'file_name' in request_args:
                table_name = request_args['table_name']
                file_name = request_args['file_name']
                bucket_name = request_args['bucket_name']
            else:
                raise Exception('Incorrect request %s' % _now())

            _restore_bigquery_table(file_name, bucket_name, table_name)
            message = 'Table restored successfully \'%s\'' % db_ref.id
            return _handle_success(db_ref, message)

        except Exception:
            message = 'Table restore error  \'%s\'. Cause: %s' % (
                db_ref.id, traceback.format_exc())
            return _handle_error(db_ref, message)
        
    else:
        return abort(405)

def _restore_bigquery_table(file_name, bucket_name, table_name):
    source_uri = 'gs://%s/%s' % (bucket_name, file_name)
    table = '%s.%s.%s' % (PROJECT_ID, BQ_DATASET, table_name)
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.AVRO, use_avro_logical_types=True)
    load_job = BQ.load_table_from_uri(
        source_uri, table, job_config=job_config) 
    load_job.result()
    
    
def _make_bigquery_backup(table_name, bucket_name):
    file_name = '%s_%s_backup.avro' % (table_name, _now())
    destination_uri = 'gs://%s/%s' % (bucket_name, file_name)
    dataset_ref = BQ.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(table_name)
    job_config = bigquery.job.ExtractJobConfig(use_avro_logical_types=True,
                                               destination_format='AVRO')
    extract_job = BQ.extract_table(
        table_ref,
        destination_uri,
        location=LOCATION,
        job_config=job_config
    )
    extract_job.result()


def _handle_success(db_ref, message):
    doc = {
        'success': True,
        'when': _now()
    }
    db_ref.set(doc)
    return _handle_response(message, 200)


def _handle_error(db_ref, message):
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