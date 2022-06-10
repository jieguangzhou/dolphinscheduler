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

package org.apache.dolphinscheduler.plugin.task.dvc;

import java.util.Date;
import java.util.UUID;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;

import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
        JSONUtils.class,
        PropertyUtils.class,
})
@PowerMockIgnore({"javax.*"})
@SuppressStaticInitializationFor("org.apache.dolphinscheduler.spi.utils.PropertyUtils")
public class DvcTaskTest {

    @Before
    public void before() throws Exception {
        PowerMockito.mockStatic(PropertyUtils.class);
    }

    public TaskExecutionContext createContext(DvcParameters dvcParameters) {
        String parameters = JSONUtils.toJsonString(dvcParameters);
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(parameters);
        Mockito.when(taskExecutionContext.getTaskLogName()).thenReturn("DvcTest");
        Mockito.when(taskExecutionContext.getExecutePath()).thenReturn("/tmp/dolphinscheduler_dvc_test");
        Mockito.when(taskExecutionContext.getTaskAppId()).thenReturn(UUID.randomUUID().toString());
        Mockito.when(taskExecutionContext.getStartTime()).thenReturn(new Date());
        Mockito.when(taskExecutionContext.getTaskTimeout()).thenReturn(10000);
        Mockito.when(taskExecutionContext.getLogPath()).thenReturn("/tmp/dolphinscheduler_dvc_test/log");
        Mockito.when(taskExecutionContext.getEnvironmentConfig()).thenReturn("export PATH=$HOME/anaconda3/bin:$PATH");

        String userName = System.getenv().get("USER");
        Mockito.when(taskExecutionContext.getTenantCode()).thenReturn(userName);

        TaskExecutionContextCacheManager.cacheTaskExecutionContext(taskExecutionContext);
        return taskExecutionContext;
    }

    private DvcTask initTask(DvcParameters parameters) {
        TaskExecutionContext taskExecutionContext = createContext(parameters);
        DvcTask dvcTask = new DvcTask(taskExecutionContext);
        dvcTask.init();
        dvcTask.getParameters().setVarPool(taskExecutionContext.getVarPool());
        return dvcTask;

    }

    @Test
    public void testDvcUpload() throws Exception{
        DvcTask dvcTask = initTask(createUploadParameters());
        System.out.println(dvcTask.buildCommand());
        dvcTask.handle();
    }

    @Test
    public void testDvcDownload() throws Exception{
        DvcTask dvcTask = initTask(createDownloadParameters());
        System.out.println(dvcTask.buildCommand());
        dvcTask.handle();
    }

    @Test
    public void testInitDvc() throws Exception{
        DvcTask dvcTask = initTask(createInitDvcParameters());
        System.out.println(dvcTask.buildCommand());
        dvcTask.handle();
    }

    private DvcParameters createUploadParameters() {
        DvcParameters parameters = new DvcParameters();
        parameters.setDvcTaskType(DvcConstants.TASK_TYPE_UPLOAD);
        parameters.setDvcRepository("git@github.com:jieguangzhou/dvc-data-repository-example");
        parameters.setDvcLoadSaveDataPath("/home/lucky/WhaleOps/MLflow-AutoML/data/test_iris.json");
        parameters.setDvcDataLocation("test_iris.json");
        parameters.setDvcVersion("iris_v2.3.1");
        parameters.setDvcMessage("add test iris data");
        return parameters;
    }

    private DvcParameters createDownloadParameters() {
        DvcParameters parameters = new DvcParameters();
        parameters.setDvcTaskType(DvcConstants.TASK_TYPE_DOWNLOAD);
        parameters.setDvcRepository("git@github.com:jieguangzhou/dvc-data-repository-example");
        parameters.setDvcLoadSaveDataPath("data");
        parameters.setDvcDataLocation("iris");
        parameters.setDvcVersion("iris_v2.3.1");
        return parameters;
    }

    private DvcParameters createInitDvcParameters() {
        DvcParameters parameters = new DvcParameters();
        parameters.setDvcTaskType(DvcConstants.TASK_TYPE_INIT_DVC);
        parameters.setDvcRepository("git@github.com:jieguangzhou/dvc-data-repository-example");
        parameters.setDvcStoreUrl("~/.dvc_test");
        return parameters;
    }
}