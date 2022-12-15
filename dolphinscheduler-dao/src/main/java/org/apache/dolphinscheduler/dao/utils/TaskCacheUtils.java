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

package org.apache.dolphinscheduler.dao.utils;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.Direct;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;

public class TaskCacheUtils {

    private TaskCacheUtils() {
        throw new IllegalStateException("Utility class");
    }

    public static final String MERGE_TAG = "-";

    /**
     * generate cache key for task instance
     * the follow message will be used to generate cache key
     * 1. task code
     * 2. task version
     * 3. task is cache
     * 4. input VarPool, from upstream task and workflow global parameters
     * @param taskInstance task instance
     * @param taskExecutionContext taskExecutionContext
     * @return cache key
     */
    public static String generateCacheKey(TaskInstance taskInstance, TaskExecutionContext taskExecutionContext) {
        List<String> keyElements = new ArrayList<>();
        keyElements.add(String.valueOf(taskInstance.getTaskCode()));
        keyElements.add(String.valueOf(taskInstance.getTaskDefinitionVersion()));
        keyElements.add(String.valueOf(taskInstance.getIsCache().getCode()));
        keyElements.add(getTaskInputVarPoolData(taskInstance, taskExecutionContext));
        String data = StringUtils.join(keyElements, "_");
        return md5(data);
    }

    /**
     * generate cache key for task instance which is cache execute
     * this key will record which cache task instance will be copied, and cache key will be used
     * tagCacheKey = sourceTaskId + "-" + cacheKey
     * @param sourceTaskId source task id
     * @param cacheKey cache key
     * @return tagCacheKey
     */
    public static String generateTagCacheKey(Integer sourceTaskId, String cacheKey) {
        return sourceTaskId + MERGE_TAG + cacheKey;
    }

    /**
     * revert cache key tag
     * @param tagCacheKey cache key
     * @return cache key
     */
    public static String revertCacheKey(String tagCacheKey) {
        if (tagCacheKey == null) {
            return "";
        }
        if (tagCacheKey.contains(MERGE_TAG)) {
            return tagCacheKey.split(MERGE_TAG)[1];
        } else {
            return tagCacheKey;
        }
    }

    /**
     * get md5 value of string
     * @param data data
     * @return md5 value
     */
    public static String md5(String data) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] md5 = md.digest(data.getBytes(StandardCharsets.UTF_8));

            StringBuilder sb = new StringBuilder();
            for (byte b : md5) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * get hash data of task input var pool
     * there are two parts of task input var pool: from upstream task and workflow global parameters
     * @param taskInstance task instance
     * taskExecutionContext taskExecutionContext
     */
    public static String getTaskInputVarPoolData(TaskInstance taskInstance, TaskExecutionContext context) {
        JsonNode taskParams = JSONUtils.parseObject(taskInstance.getTaskParams());

        // The set of input values considered from localParams in the taskParams
        Set<String> propertyInSet = JSONUtils.toList(taskParams.get("localParams").toString(), Property.class).stream()
                .filter(property -> property.getDirect().equals(Direct.IN))
                .map(Property::getProp).collect(Collectors.toSet());

        // The set of input values considered from `${var}` form task definition
        propertyInSet.addAll(getScriptVarInSet(taskInstance));

        // var pool value from upstream task
        List<Property> varPool = JSONUtils.toList(taskInstance.getVarPool(), Property.class);

        // var pool value from workflow global parameters
        if (context.getPrepareParamsMap() != null) {
            Set<String> taskVarPoolSet = varPool.stream().map(Property::getProp).collect(Collectors.toSet());
            List<Property> globalContextVarPool = context.getPrepareParamsMap().entrySet().stream()
                    .filter(entry -> !taskVarPoolSet.contains(entry.getKey()))
                    .map(Map.Entry::getValue)
                    .collect(Collectors.toList());
            varPool.addAll(globalContextVarPool);
        }

        // only consider var pool value which is in propertyInSet
        varPool = varPool.stream()
                .filter(property -> property.getDirect().equals(Direct.IN))
                .filter(property -> propertyInSet.contains(property.getProp()))
                .sorted(Comparator.comparing(Property::getProp))
                .collect(Collectors.toList());
        return JSONUtils.toJsonString(varPool);
    }

    /**
     * get var in set from task definition
     * @param taskInstance task instance
     * @return var in set
     */
    public static List<String> getScriptVarInSet(TaskInstance taskInstance) {
        Pattern pattern = Pattern.compile("\\$\\{(.+?)\\}");
        Matcher matcher = pattern.matcher(taskInstance.getTaskParams());

        List<String> varInSet = new ArrayList<>();
        while (matcher.find()) {
            varInSet.add(matcher.group(1));
        }
        return varInSet;
    }

}
