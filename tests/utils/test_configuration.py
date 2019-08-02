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

import configparser
from io import StringIO
import textwrap
import unittest

from airflow.utils.configuration import update_latest_by_user


def _get_configparser(config):
    cp = configparser.ConfigParser()
    cp.read_file(StringIO(config))
    return cp


class TestConfiguration(unittest.TestCase):

    def test_update_if_not_exist(self):
        config_user = textwrap.dedent("""\
            [section1]
            a = True
        """)
        cp_user = _get_configparser(config_user)

        config_latest = textwrap.dedent("""\
            [section1]
            b = True
        """)
        cp_latest = _get_configparser(config_latest)

        cp_merge = update_latest_by_user(cp_user=cp_user, cp_latest=cp_latest)

        self.assertEqual(True, cp_merge.getboolean('section1', 'a'))
        self.assertEqual(True, cp_merge.getboolean('section1', 'b'))

    def test_update_favor_user(self):
        config_user = textwrap.dedent("""\
            [section1]
            a = True
        """)
        cp_user = _get_configparser(config_user)

        config_latest = textwrap.dedent("""\
            [section1]
            a = False
        """)
        cp_latest = _get_configparser(config_latest)

        cp_merge = update_latest_by_user(cp_user=cp_user, cp_latest=cp_latest)

        self.assertEqual(True, cp_merge.getboolean('section1', 'a'))
