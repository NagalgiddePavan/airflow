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

import unittest
from unittest.mock import patch

from airflow.hooks import base_hook
from airflow.hooks.base_hook import BaseHook


class TestBaseHook(unittest.TestCase):

    @patch.dict('os.environ', {"OVERRIDE_AIRFLOW_CONN_XXX": "s3://yyy"})
    def test_conn_env_prefix_from_cfg(self):
        conn_env_prefix_prev = base_hook.CONN_ENV_PREFIX
        try:
            base_hook.CONN_ENV_PREFIX = 'OVERRIDE_AIRFLOW_CONN_'

            hook = BaseHook(source=None)

            conn = hook.get_connection('xxx')
            self.assertEqual(conn.host, 'yyy')
        finally:
            base_hook.CONN_ENV_PREFIX = conn_env_prefix_prev
