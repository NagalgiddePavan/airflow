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

import os
import json
from tempfile import mkstemp

from configparser import ConfigParser

from airflow import configuration as conf


def tmp_configuration_copy(chmod=0o600, include_env=True, include_cmds=True):
    """
    Returns a path for a temporary file including a full copy of the configuration
    settings.
    :return: a path to a temporary file
    """
    cfg_dict = conf.as_dict(display_sensitive=True, raw=True)
    temp_fd, cfg_path = mkstemp()

    with os.fdopen(temp_fd, 'w') as temp_file:
        # Set the permissions before we write anything to it.
        if chmod is not None:
            os.fchmod(temp_fd, chmod)
        json.dump(cfg_dict, temp_file)

    return cfg_path


def update_latest_by_user(cp_user: ConfigParser, cp_latest: ConfigParser):
    """
    Merge the user's config with latest config.
    If there is a conflict, user's config is favored.

    :param cp_user: user's airflow.cfg
    :param cp_latest: latest airflow.cfg
    """
    cp_merge = cp_latest
    for section in cp_user.sections():
        if not cp_merge.has_section(section):
            cp_merge.add_section(section)
        for k, v in cp_user.items(section):
            cp_merge.set(section, k, v)
    return cp_merge
