#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

data_path=${data_path}
export MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI}
echo $data_path
repo=${repo}
mlflow run $repo -P algorithm=${algorithm} -P data_path=$data_path -P params="${params}" -P param_file=${param_file} -P search_params="${search_params}" -P model_name=${model_name} --experiment-name=${experiment_name}

echo "training finish"
