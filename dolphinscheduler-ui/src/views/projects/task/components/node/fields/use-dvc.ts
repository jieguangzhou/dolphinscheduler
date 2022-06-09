/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { useI18n } from 'vue-i18n'
import type { IJsonItem } from '../types'
import { watch, ref } from 'vue'

export const MLFLOW_TASK_TYPE = [
  {
    label: 'Upload',
    value: 'Upload'
  },
  {
    label: 'Download',
    value: 'Download'
  },
  {
    label: 'Init DVC',
    value: 'Init DVC'
  }
]

export function useDvc(model: { [field: string]: any }): IJsonItem[] {
  const { t } = useI18n()

  const dvcLoadSaveDataPathSpan = ref(0)
  const dvcDataLocationSpan = ref(0)
  const dvcVersionSpan = ref(0)
  const dvcMessageSpan = ref(0)
  const dvcStoreUrlSpan = ref(0)

  const setFlag = () => {
    model.isUpload = model.dvcTaskType === 'Upload' ? true : false
    model.isDownload = model.dvcTaskType === 'Download' ? true : false
    model.isInit = model.dvcTaskType === 'Init DVC' ? true : false
  }

  const resetSpan = () => {
    dvcLoadSaveDataPathSpan.value = model.isUpload || model.isDownload ? 24 : 0
    dvcDataLocationSpan.value = model.isUpload || model.isDownload ? 24 : 0
    dvcVersionSpan.value = model.isUpload || model.isDownload ? 24 : 0
    dvcMessageSpan.value = model.isUpload ? 24 : 0
    dvcStoreUrlSpan.value = model.isInit ? 24 : 0
  }

  watch(
    () => [model.dvcTaskType],
    () => {
      setFlag()
      resetSpan()
    }
  )
  setFlag()
  resetSpan()

  return [
    {
      type: 'input',
      field: 'dvcRepository',
      name: 'dvcRepository',
      span: 12,
      props: {
        placeholder: t('project.node.mlflow_mlflowTrackingUri_tips')
      },
      validate: {
        trigger: ['input', 'blur'],
        required: false,
        validator(validate: any, value: string) {
          if (!value) {
            return new Error(
              t('project.node.mlflow_mlflowTrackingUri_error_tips')
            )
          }
        }
      }
    },
    {
      type: 'select',
      field: 'dvcTaskType',
      name: 'dvcTaskType',
      span: 12,
      options: MLFLOW_TASK_TYPE
    },
    {
      type: 'input',
      field: 'dvcDataLocation',
      name: 'dvcDataLocation',
      props: {
        placeholder: 'dvcDataLocation'
      },
      span: dvcDataLocationSpan,
      validate: {
        trigger: ['input', 'blur'],
        required: false
      }
    },
    {
      type: 'input',
      field: 'dvcLoadSaveDataPath',
      name: 'dvcLoadSaveDataPath',
      span: dvcLoadSaveDataPathSpan
    },
    {
      type: 'input',
      field: 'dvcVersion',
      name: 'dvcVersion',
      span: dvcVersionSpan
    },
    {
      type: 'input',
      field: 'dvcMessage',
      name: 'dvcMessage',
      span: dvcMessageSpan
    },
    {
      type: 'input',
      field: 'dvcStoreUrl',
      name: 'dvcStoreUrl',
      span: dvcStoreUrlSpan
    }
  ]
}
