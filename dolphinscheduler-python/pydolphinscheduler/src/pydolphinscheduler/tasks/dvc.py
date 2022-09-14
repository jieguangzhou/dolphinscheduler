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

"""Task dvc."""

from pydolphinscheduler.constants import TaskType
from pydolphinscheduler.core.task import Task


class DvcTaskType(str):
    """Constants for dvc task type."""

    INIT = "Init DVC"
    DOWNLOAD = "Download"
    UPLOAD = "Upload"


class DVCInit(Task):
    """Task DVC Init object, declare behavior for DVC Init task to dolphinscheduler."""

    dvc_task_type = DvcTaskType.INIT

    _task_custom_attr = {
        "dvc_task_type",
        "dvc_repository",
        "dvc_store_url",
    }

    def __init__(self, name: str, repository: str, store_url: str, *args, **kwargs):
        super().__init__(name, TaskType.DVC, *args, **kwargs)
        self.dvc_repository = repository
        self.dvc_store_url = store_url


class DVCDownload(Task):
    """Task DVC Download object, declare behavior for DVC Download task to dolphinscheduler."""

    dvc_task_type = DvcTaskType.DOWNLOAD

    _task_custom_attr = {
        "dvc_task_type",
        "dvc_repository",
        "dvc_load_save_data_path",
        "dvc_data_location",
        "dvc_version",
    }

    def __init__(
        self,
        name: str,
        repository: str,
        data_path_in_dvc_repository: str,
        data_path_in_worker: str,
        version: str,
        *args,
        **kwargs
    ):
        super().__init__(name, TaskType.DVC, *args, **kwargs)
        self.dvc_repository = repository
        self.dvc_data_location = data_path_in_dvc_repository
        self.dvc_load_save_data_path = data_path_in_worker
        self.dvc_version = version


class DVCUpload(Task):
    """Task DVC Upload object, declare behavior for DVC Upload task to dolphinscheduler."""

    dvc_task_type = DvcTaskType.UPLOAD

    _task_custom_attr = {
        "dvc_task_type",
        "dvc_repository",
        "dvc_load_save_data_path",
        "dvc_data_location",
        "dvc_version",
        "dvc_message",
    }

    def __init__(
        self,
        name: str,
        repository: str,
        data_path_in_worker: str,
        data_path_in_dvc_repository: str,
        version: str,
        message: str,
        *args,
        **kwargs
    ):
        super().__init__(name, TaskType.DVC, *args, **kwargs)
        self.dvc_repository = repository
        self.dvc_data_location = data_path_in_dvc_repository
        self.dvc_load_save_data_path = data_path_in_worker
        self.dvc_version = version
        self.dvc_message = message
