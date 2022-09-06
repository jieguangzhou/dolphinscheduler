package org.apache.dolphinscheduler.plugin.task.dms;

import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.spi.utils.PropertyUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationService;
import com.amazonaws.services.databasemigrationservice.AWSDatabaseMigrationServiceClientBuilder;
import com.amazonaws.services.databasemigrationservice.model.CreateReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.CreateReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.DeleteReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.DeleteReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.DescribeConnectionsRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeConnectionsResult;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksRequest;
import com.amazonaws.services.databasemigrationservice.model.DescribeReplicationTasksResult;
import com.amazonaws.services.databasemigrationservice.model.Filter;
import com.amazonaws.services.databasemigrationservice.model.InvalidResourceStateException;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTask;
import com.amazonaws.services.databasemigrationservice.model.ReplicationTaskStats;
import com.amazonaws.services.databasemigrationservice.model.ResourceNotFoundException;
import com.amazonaws.services.databasemigrationservice.model.StartReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.StartReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskRequest;
import com.amazonaws.services.databasemigrationservice.model.StopReplicationTaskResult;
import com.amazonaws.services.databasemigrationservice.model.Tag;
import com.amazonaws.services.databasemigrationservice.model.TestConnectionRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import lombok.Data;
import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.fasterxml.jackson.databind.DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL;
import static com.fasterxml.jackson.databind.MapperFeature.REQUIRE_SETTERS_FOR_GETTERS;

@Data
public class DmsHook {
    protected final Logger logger = LoggerFactory.getLogger(String.format(TaskConstants.TASK_LOG_LOGGER_NAME_FORMAT, getClass()));
    private static final ObjectMapper objectMapper =
        new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false).configure(ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true).configure(READ_UNKNOWN_ENUM_VALUES_AS_NULL, true)
            .configure(REQUIRE_SETTERS_FOR_GETTERS, true).setPropertyNamingStrategy(new PropertyNamingStrategy.UpperCamelCaseStrategy());
    private AWSDatabaseMigrationService client;
    private String replicationTaskIdentifier;
    private String sourceEndpointArn;
    private String targetEndpointArn;
    private String replicationInstanceArn;
    private String migrationType;
    private String tableMappings;
    private String replicationTaskSettings;
    private Date cdcStartTime;
    private String cdcStartPosition;
    private String cdcStopPosition;
    private List<Tag> tags;
    private String taskData;
    private String resourceIdentifier;
    private String replicationTaskArn;
    private String startReplicationTaskType;

    public DmsHook(AWSDatabaseMigrationService client) {
        this.client = client;
    }

    public CreateReplicationTaskRequest createCreateReplicationTaskRequest() {
        return new CreateReplicationTaskRequest()
            .withReplicationTaskIdentifier(replicationTaskIdentifier)
            .withSourceEndpointArn(sourceEndpointArn)
            .withTargetEndpointArn(targetEndpointArn)
            .withReplicationInstanceArn(replicationInstanceArn)
            .withMigrationType(migrationType)
            .withTableMappings(tableMappings)
            .withReplicationTaskSettings(replicationTaskSettings)
            .withCdcStartTime(cdcStartTime)
            .withCdcStartPosition(cdcStartPosition)
            .withCdcStopPosition(cdcStopPosition)
            .withTags(tags)
            .withTaskData(taskData)
            .withResourceIdentifier(resourceIdentifier);
    }

    public CreateReplicationTaskRequest createCreateReplicationTaskRequest(String jsonData) throws Exception {
        return objectMapper.readValue(jsonData, CreateReplicationTaskRequest.class);
    }

    public Boolean createReplicationTask(CreateReplicationTaskRequest request) {
        logger.info("createReplicationTask ......");
        CreateReplicationTaskResult response = client.createReplicationTask(request);
        replicationTaskArn = response.getReplicationTask().getReplicationTaskArn();
        replicationTaskIdentifier = response.getReplicationTask().getReplicationTaskIdentifier();
        logger.info(replicationTaskArn);
        return awaitReplicationTaskStatus(STATUS.READY);
    }

    public StartReplicationTaskRequest createStartReplicationTaskRequest() {
        return new StartReplicationTaskRequest()
            .withReplicationTaskArn(replicationTaskArn)
            .withStartReplicationTaskType(startReplicationTaskType)
            .withCdcStartTime(cdcStartTime)
            .withCdcStartPosition(cdcStartPosition)
            .withCdcStopPosition(cdcStopPosition);
    }

    public StartReplicationTaskRequest createStartReplicationTaskRequest(String jsonData) throws Exception{
        return objectMapper.readValue(jsonData, StartReplicationTaskRequest.class);
    }

    public Boolean startReplicationTask(StartReplicationTaskRequest request) {
        logger.info("startReplicationTask ......");
        StartReplicationTaskResult response = client.startReplicationTask(request);
        return awaitReplicationTaskStatus(STATUS.RUNNING);
    }

    public Boolean checkFinishedReplicationTask() {
        logger.info("checkFinishedReplicationTask ......");
        awaitReplicationTaskStatus(STATUS.STOPPED);
        String stopReason = describeReplicationTasks().getReplicationTasks().get(0).getStopReason();
        return stopReason.endsWith(STATUS.FINISH_END_TOKEN);
    }

    public Boolean stopReplicationTask() {
        logger.info("stopReplicationTask ......");
        StopReplicationTaskRequest request = new StopReplicationTaskRequest()
            .withReplicationTaskArn(replicationTaskArn);
        StopReplicationTaskResult response = client.stopReplicationTask(request);
        return awaitReplicationTaskStatus(STATUS.STOPPED);
    }

    public Boolean deleteReplicationTask() {
        logger.info("deleteReplicationTask ......");
        DeleteReplicationTaskRequest request = new DeleteReplicationTaskRequest()
            .withReplicationTaskArn(replicationTaskArn);
        DeleteReplicationTaskResult response = client.deleteReplicationTask(request);
        Boolean isDeleteSuccessfully;
        try {
            isDeleteSuccessfully = awaitReplicationTaskStatus(STATUS.DELETE);
        } catch (ResourceNotFoundException e) {
            isDeleteSuccessfully = true;
        }
        return isDeleteSuccessfully;
    }

    public Boolean testConnectionSourceEndpoint() {
        logger.info("Test connect source endpoint");
        return testConnection(replicationInstanceArn, sourceEndpointArn);
    }

    public Boolean testConnectionTargetEndpoint() {
        logger.info("Test connect target endpoint");
        return testConnection(replicationInstanceArn, targetEndpointArn);
    }

    public Boolean testConnection(String replicationInstanceArn, String endpointArn) {
        TestConnectionRequest request = new TestConnectionRequest().
            withReplicationInstanceArn(replicationInstanceArn)
            .withEndpointArn(endpointArn);
        try {
            client.testConnection(request);
        } catch (InvalidResourceStateException e) {
            logger.info(e.getErrorMessage());
        }

        return awaitConnectSuccess(replicationInstanceArn, endpointArn);
    }

    public Boolean awaitConnectSuccess(String replicationInstanceArn, String endpointArn) {
        Filter instanceFilters = new Filter().withName(AWS_KEY.REPLICATION_INSTANCE_ARN).withValues(replicationInstanceArn);
        Filter endpointFilters = new Filter().withName(AWS_KEY.ENDPOINT_ARN).withValues(endpointArn);
        DescribeConnectionsRequest request = new DescribeConnectionsRequest().withFilters(endpointFilters, instanceFilters)
            .withMarker("");
        while (true) {
//            ThreadUtils.sleep(1);
            DescribeConnectionsResult response = client.describeConnections(request);
            String status = response.getConnections().get(0).getStatus();
            if (status.equals(STATUS.SUCCESSFUL)) {
                logger.info("Connect successful");
                return true;
            } else if (!status.equals(STATUS.TESTING)) {
                break;
            }
        }
        logger.info("Connect error");
        return false;
    }

    public DescribeReplicationTasksResult describeReplicationTasks() {
        Filter replicationTaskFilter = new Filter().withName(AWS_KEY.REPLICATION_TASK_ARN).withValues(replicationTaskArn);
        DescribeReplicationTasksRequest request = new DescribeReplicationTasksRequest().withFilters(replicationTaskFilter).withMaxRecords(20).withMarker("");
        return client.describeReplicationTasks(request);
    }

    public Boolean awaitReplicationTaskStatus(String exceptStatus, String... stopStatus) {
        Filter replicationTaskFilter = new Filter().withName(AWS_KEY.REPLICATION_TASK_ARN).withValues(replicationTaskArn);
        DescribeReplicationTasksRequest request = new DescribeReplicationTasksRequest().withFilters(replicationTaskFilter).withMaxRecords(20).withMarker("");
        List<String> stopStatusSet = Arrays.asList(stopStatus);
        Integer lastPercent = 0;
        while (true) {
//            ThreadUtils.sleep(1);
            DescribeReplicationTasksResult response = client.describeReplicationTasks(request);
            ReplicationTask replicationTask = response.getReplicationTasks().get(0);
            String status = replicationTask.getStatus();

            if (status.equals(STATUS.RUNNING) || status.equals(STATUS.STOPPED)) {
                ReplicationTaskStats taskStats = replicationTask.getReplicationTaskStats();
                Integer percent;
                if (taskStats != null) {
                    percent = taskStats.getFullLoadProgressPercent();
                } else {
                    percent = 0;
                }
                if (!lastPercent.equals(percent)) {
                    String runningMessage = String.format("fullLoadProgressPercent: %s ", percent);
                    logger.info(runningMessage);
                }
                lastPercent = percent;
            }

            if (exceptStatus.equals(status)) {
                logger.info("success");
                return true;
            } else if (stopStatusSet.contains(status)) {
                break;
            }
        }
        logger.info("error");
        return false;
    }

    public static AWSDatabaseMigrationService createClient() {
        final String awsAccessKeyId = PropertyUtils.getString(TaskConstants.AWS_ACCESS_KEY_ID);
        final String awsSecretAccessKey = PropertyUtils.getString(TaskConstants.AWS_SECRET_ACCESS_KEY);
        final String awsRegion = PropertyUtils.getString(TaskConstants.AWS_REGION);
        final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
        final AWSCredentialsProvider awsCredentialsProvider = new AWSStaticCredentialsProvider(basicAWSCredentials);

        // create a DMS client
        return AWSDatabaseMigrationServiceClientBuilder.standard()
            .withCredentials(awsCredentialsProvider)
            .withRegion(awsRegion)
            .build();
    }

    public static class STATUS {
        public static final String DELETE = "delete";
        public static final String READY = "ready";
        public static final String RUNNING = "running";
        public static final String STOPPED = "stopped";
        public static final String SUCCESSFUL = "successful";
        public static final String TESTING = "testing";
        public static final String FINISH_END_TOKEN = "FINISHED";
    }

    public static class AWS_KEY {
        public static final String REPLICATION_TASK_ARN = "replication-task-arn";
        public static final String REPLICATION_INSTANCE_ARN = "replication-instance-arn";
        public static final String ENDPOINT_ARN = "endpoint-arn";
    }

    public static class START_TYPE {
        public static final String START_REPLICATION = "start-replication";
        public static final String RELOAD_TARGET = "reload-target";
    }
}
