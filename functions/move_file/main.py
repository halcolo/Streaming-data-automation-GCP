'''
This Cloud function moves a file from one bucket to another
'''
import base64
import os
import logging
from datetime import datetime
import pytz
from google.cloud import storage

CS = storage.Client()


def move_file(data, context):
    '''This function is triggered from a Cloud Pub/Sub'''
    message = base64.b64decode(data['data']).decode('utf-8')
    file_name = data['attributes']['file_name']

    source_bucket_name = os.getenv('SOURCE_BUCKET')
    source_bucket = CS.get_bucket(source_bucket_name)
    source_blob = source_bucket.blob(file_name)

    destination_bucket_name = os.getenv('DESTINATION_BUCKET')
    destination_bucket = CS.get_bucket(destination_bucket_name)

    name_splited = file_name.split('.')
    name_splited.insert(1, _now())
    new_file_name = '%s.%s' % ('_'.join(name_splited[:-1]), name_splited[-1])
    source_bucket.copy_blob(source_blob, destination_bucket, new_file_name)
    source_blob.delete()

    logging.info('File \'%s\' moved from \'%s\' to \'%s\': \'%s\'',
                 file_name,
                 source_bucket_name,
                 destination_bucket_name,
                 message)

def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')