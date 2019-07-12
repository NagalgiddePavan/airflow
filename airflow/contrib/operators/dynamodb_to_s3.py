# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from copy import copy
from multiprocessing import Process, JoinableQueue
from os.path import getsize
from tempfile import NamedTemporaryFile
from uuid import uuid4

from boto.compat import json

from airflow.contrib.hooks.aws_dynamodb_hook import AwsDynamoDBHook
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.baseoperator import BaseOperator


MAX_QUEUE_LEN = 1000


def _convert_item_to_json_bytes(item):
    return (json.dumps(item) + '\n').encode('utf-8')


def _upload_file_to_s3(file_obj, bucket_name, s3_key_prefix):
    s3_client = S3Hook().get_conn()
    file_obj.seek(0)
    s3_client.upload_file(
        Filename=file_obj.name,
        Bucket=bucket_name,
        Key=s3_key_prefix + str(uuid4()),
    )


class S3Uploader(Process, LoggingMixin):
    """
    Reads items from the queue, do some transformation, write to disk, upload to s3.

    """

    def __init__(self, item_queue, transform_func, file_size, s3_bucket_name, s3_key_prefix):
        super(S3Uploader, self).__init__()
        self.item_queue = item_queue
        self.transform_func = transform_func
        self.file_size = file_size
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix

    def run(self):
        f = NamedTemporaryFile()
        try:
            while True:
                item = self.item_queue.get()
                try:
                    if item is None:
                        self.log.debug('Got poisoned and die.')
                        break
                    f.write(self.transform_func(item))

                finally:
                    self.item_queue.task_done()

                # Upload the file to S3 if reach file size limit
                if getsize(f.name) >= self.file_size:
                    _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
                    f.close()
                    f = NamedTemporaryFile()
        finally:
            _upload_file_to_s3(f, self.s3_bucket_name, self.s3_key_prefix)
            f.close()


class DynamoDBScanner(Process, LoggingMixin):
    """
    Scan DynamoDB table and put items in a queue.
    """

    def __init__(self, table_name, scan_kwargs, item_queue):
        super(DynamoDBScanner, self).__init__()
        self.table_name = table_name
        self.scan_kwargs = scan_kwargs
        self.item_queue = item_queue

    def run(self):
        table = AwsDynamoDBHook().get_conn().Table(self.table_name)
        scan_kwargs = copy(self.scan_kwargs) if self.scan_kwargs else {}
        while True:
            response = table.scan(**scan_kwargs)
            items = response['Items']
            for item in items:
                self.item_queue.put(item)
                self.log.info(item)

            if 'LastEvaluatedKey' not in response:
                # no more items to scan
                break

            last_evaluated_key = response['LastEvaluatedKey']
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key


class DynamoDBToS3Operator(BaseOperator):

    def __init__(self,
                 dynamodb_table_name,
                 s3_bucket_name,
                 file_size,
                 dynamodb_scan_kwargs=None,
                 s3_key_prefix='',
                 num_s3_uploader=1,
                 process_func=_convert_item_to_json_bytes,
                 max_queue_len=MAX_QUEUE_LEN,
                 *args, **kwargs):
        super(DynamoDBToS3Operator, self).__init__(*args, **kwargs)
        self.file_size = file_size
        self.process_func = process_func
        self.num_s3_uploader = num_s3_uploader
        self.s3_uploaders = []
        self.dynamodb_table_name = dynamodb_table_name
        self.dynamodb_scan_kwargs = dynamodb_scan_kwargs
        self.dynamodb_scanner = None
        self.s3_bucket_name = s3_bucket_name
        self.s3_key_prefix = s3_key_prefix
        self.max_queue_len = max_queue_len

    def execute(self, context):
        item_queue = JoinableQueue(maxsize=self.max_queue_len)
        try:
            self.dynamodb_scanner = DynamoDBScanner(
                table_name=self.dynamodb_table_name,
                scan_kwargs=self.dynamodb_scan_kwargs,
                item_queue=item_queue,
            )
            self.dynamodb_scanner.start()

            for _ in range(self.num_s3_uploader):
                s3_uploader = S3Uploader(
                    item_queue=item_queue,
                    transform_func=self.process_func,
                    file_size=self.file_size,
                    s3_bucket_name=self.s3_bucket_name,
                    s3_key_prefix=self.s3_key_prefix,
                )
                s3_uploader.start()
                self.s3_uploaders.append(s3_uploader)

            # Wait until dynamodb scan finish.
            self.dynamodb_scanner.join()

            self.log.info('Sending poison pills to kill workers.')
            for _ in range(self.num_s3_uploader):
                item_queue.put(None)

            item_queue.join()
        except Exception:
            for s3_uploader in self.s3_uploaders:
                s3_uploader.terminate()

            self.dynamodb_scanner.terminate()
            self.dynamodb_scanner.join()
            raise
        finally:
            for s3_uploader in self.s3_uploaders:
                s3_uploader.join()
            self.dynamodb_scanner.join()
            # TODO: Cleanup the S3 files...

    def on_kill(self):
        for s3_uploader in self.s3_uploaders:
            s3_uploader.terminate()
        self.dynamodb_scanner.terminate()
