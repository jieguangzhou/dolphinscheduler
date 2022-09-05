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

package org.apache.dolphinscheduler.plugin.task.dms;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;
import static com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

public class DmsTask extends AbstractTaskExecutor {

    private static final ObjectMapper objectMapper =
        new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false).configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true).configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .configure(REQUIRE_SETTERS_FOR_GETTERS, true).setPropertyNamingStrategy(new PropertyNamingStrategy.UpperCamelCaseStrategy());
    /**
     * taskExecutionContext
     */
    private final TaskExecutionContext taskExecutionContext;
    /**
     * Dms parameters
     */
    private DmsParameters parameters;
    private DmsHook dmsHook;

    public DmsTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;

    }

    @Override
    public void init() {
        logger.info("Dms task params {}", taskExecutionContext.getTaskParams());

        parameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), DmsParameters.class);
        initDmsHook();

    }

    @Override
    public void handle() throws TaskException {
        try {
            int exitStatusCode = runDmsReplicationTask();
            setExitStatusCode(exitStatusCode);
        } catch (Exception e) {
            setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw new TaskException("DMS task error", e);
        }
    }

    public int runDmsReplicationTask() {
        int exitStatusCode;
        exitStatusCode = checkCreateReplicationTask();
        if (exitStatusCode == TaskConstants.EXIT_CODE_SUCCESS) {
            exitStatusCode = startReplicationTask();
        }
        return exitStatusCode;
    }

    public int checkCreateReplicationTask() {
        if (parameters.getIsRestartTask()) {
            return TaskConstants.EXIT_CODE_SUCCESS;
        }

        Boolean isCreateSuccessfully = dmsHook.createReplicationTask();
        if (!isCreateSuccessfully) {
            return TaskConstants.EXIT_CODE_FAILURE;
        } else {
            return TaskConstants.EXIT_CODE_SUCCESS;
        }
    }

    public int startReplicationTask() {
        Boolean isStartSuccessfully = dmsHook.startReplicationTask();
        if (!isStartSuccessfully) {
            return TaskConstants.EXIT_CODE_FAILURE;
        }

        Boolean isFinishedSuccessfully = dmsHook.checkFinishedReplicationTask();
        if (!isFinishedSuccessfully) {
            return TaskConstants.EXIT_CODE_FAILURE;
        } else {
            return TaskConstants.EXIT_CODE_SUCCESS;
        }
    }

    public void initDmsHook() {

        dmsHook = new DmsHook();
        if (parameters.getIsRestartTask()) {
            dmsHook.setReplicationTaskArn(parameters.getReplicationTaskArn());
            dmsHook.setStartReplicationTaskType(parameters.getStartReplicationTaskType());
        }else {
            dmsHook.setReplicationTaskIdentifier(parameters.getReplicationTaskIdentifier());
            dmsHook.setSourceEndpointArn(parameters.getSourceEndpointArn());
            dmsHook.setTargetEndpointArn(parameters.getTargetEndpointArn());
            dmsHook.setReplicationInstanceArn(parameters.getReplicationInstanceArn());
            dmsHook.setMigrationType(parameters.getMigrationType());
            dmsHook.setTableMappings(parameters.getTableMappings());
            dmsHook.setReplicationTaskSettings(parameters.getReplicationTaskSettings());
            dmsHook.setTags(parameters.getTags());
            dmsHook.setTaskData(parameters.getTaskData());
            dmsHook.setResourceIdentifier(parameters.getResourceIdentifier());
        }
        dmsHook.setCdcStartTime(parameters.getCdcStartTime());
        dmsHook.setCdcStartPosition(parameters.getCdcStartPosition());
        dmsHook.setCdcStopPosition(parameters.getCdcStopPosition());
    }

    @Override
    public DmsParameters getParameters() {
        return parameters;
    }

    @Override
    public void cancelApplication(boolean cancelApplication) {
        // stop pipeline
        dmsHook.stopReplicationTask();
//        dmsHook.deleteReplicationTask();
    }

}
