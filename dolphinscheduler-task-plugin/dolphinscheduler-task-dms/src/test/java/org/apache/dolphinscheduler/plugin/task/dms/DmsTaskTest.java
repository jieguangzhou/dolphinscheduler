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

import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.doCallRealMethod;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContextCacheManager;
import org.apache.dolphinscheduler.spi.utils.JSONUtils;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationServiceClientBuilder;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksResult;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.Tag;

@RunWith(PowerMockRunner.class)
@PrepareForTest({JSONUtils.class, PropertyUtils.class,})
@PowerMockIgnore({"javax.*"})
@SuppressStaticInitializationFor("org.apache.dolphinscheduler.spi.utils.PropertyUtils")
public class DmsTaskTest {

    TaskExecutionContext taskExecutionContext;

    @Before
    public void before() {
        PowerMockito.mockStatic(PropertyUtils.class);
        taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
    }


    @Test
    public void testHookAwaitReplicationTaskStatus() {

        DmsHook dmsHook = mock(DmsHook.class);
        AWSDatabaseMigrationService client = mock(AWSDatabaseMigrationService.class);
        when(dmsHook.createClient()).thenReturn(client);


        DescribeReplicationTasksResult describeReplicationTasksResult = mock(DescribeReplicationTasksResult.class);
        when(client.describeReplicationTasks(any())).thenReturn(describeReplicationTasksResult);

        ReplicationTask replicationTask = mock(ReplicationTask.class);
        List<ReplicationTask> replicationTasks = new ArrayList<>();
        replicationTasks.add(replicationTask);
        when(describeReplicationTasksResult.getReplicationTasks()).thenReturn(replicationTasks);

        when(replicationTask.getStatus()).thenReturn(DmsHook.STATUS.READY);
        System.out.println(dmsHook.awaitReplicationTaskStatus(DmsHook.STATUS.READY));
        System.out.println(dmsHook.awaitReplicationTaskStatus(DmsHook.STATUS.STOPPED));
        System.out.println(dmsHook.awaitReplicationTaskStatus(DmsHook.STATUS.SUCCESSFUL));
//        Assert.assertTrue(dmsHook.awaitReplicationTaskStatus(DmsHook.STATUS.READY));

    }


    @Test
    public void testCreateAndStartTask() {
        DmsParameters dmsParameters = new DmsParameters();
        dmsParameters.setReplicationTaskIdentifier("task0");
        dmsParameters.setSourceEndpointArn("arn:aws:dms:ap-southeast-1:511640773671:endpoint:Z7SUEAL273SCT7OCPYNF5YNDHJDDFRATGNQISOQ");
        dmsParameters.setTargetEndpointArn("arn:aws:dms:ap-southeast-1:511640773671:endpoint:aws-mysql57-target");
        dmsParameters.setReplicationInstanceArn("arn:aws:dms:ap-southeast-1:511640773671:rep:dms2c2g");
        dmsParameters.setMigrationType("full-load");
        dmsParameters.setStartReplicationTaskType("start-replication");
        DmsTask dmsTask = initTask(dmsParameters);
    }

    @Test
    public void testRestartTask() {
        DmsParameters dmsParameters = new DmsParameters();
        dmsParameters.setIsRestartTask(true);
        dmsParameters.setReplicationTaskArn("arn:aws:dms:ap-southeast-1:511640773671:task:REKD6S2SXE6BZS7HUO4PGGFEMANQD25I6WPI4YI");
        dmsParameters.setStartReplicationTaskType("reload-target");
        DmsTask dmsTask = initTask(dmsParameters);
    }

    private DmsTask initTask(DmsParameters dmsParameters) {
        TaskExecutionContext taskExecutionContext = createContext(dmsParameters);
        DmsTask dmsTask = new DmsTask(taskExecutionContext);
        dmsTask.init();
        return dmsTask;
    }

    public TaskExecutionContext createContext(DmsParameters dmsParameters) {
        String parameters = JSONUtils.toJsonString(dmsParameters);
        TaskExecutionContext taskExecutionContext = Mockito.mock(TaskExecutionContext.class);
        Mockito.when(taskExecutionContext.getTaskParams()).thenReturn(parameters);
        return taskExecutionContext;
    }

}


