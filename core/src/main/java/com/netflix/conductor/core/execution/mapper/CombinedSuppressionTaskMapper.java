/*
 * Copyright 2022 Conductor Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.core.execution.mapper;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;

/** Maps a {@link WorkflowTask} of type {@code COMBINED_SUPPRESSION} to a server-side system task. */
@Component
public class CombinedSuppressionTaskMapper implements TaskMapper {

    public static final String TASK_TYPE = "COMBINED_SUPPRESSION";

    private static final Logger LOGGER =
            LoggerFactory.getLogger(CombinedSuppressionTaskMapper.class);

    private final ParametersUtils parametersUtils;
    private final MetadataDAO metadataDAO;

    @Autowired
    public CombinedSuppressionTaskMapper(
            ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
        this.parametersUtils = parametersUtils;
        this.metadataDAO = metadataDAO;
    }

    @Override
    public String getTaskType() {
        return TASK_TYPE;
    }

    @Override
    public List<TaskModel> getMappedTasks(TaskMapperContext taskMapperContext)
            throws TerminateWorkflowException {

        LOGGER.debug("TaskMapperContext {} in CombinedSuppressionTaskMapper", taskMapperContext);

        WorkflowTask workflowTask = taskMapperContext.getWorkflowTask();
        workflowTask.getInputParameters().put("asyncComplete", workflowTask.isAsyncComplete());

        WorkflowModel workflowModel = taskMapperContext.getWorkflowModel();
        String taskId = taskMapperContext.getTaskId();
        int retryCount = taskMapperContext.getRetryCount();

        TaskDef taskDefinition =
                Optional.ofNullable(taskMapperContext.getTaskDefinition())
                        .orElseGet(() -> metadataDAO.getTaskDef(workflowTask.getName()));

        Map<String, Object> input =
                parametersUtils.getTaskInputV2(
                        workflowTask.getInputParameters(), workflowModel, taskId, taskDefinition);

        TaskModel taskModel = taskMapperContext.createTaskModel();
        taskModel.setInputData(input);
        taskModel.setStatus(TaskModel.Status.SCHEDULED);
        taskModel.setRetryCount(retryCount);
        taskModel.setCallbackAfterSeconds(workflowTask.getStartDelay());

        if (Objects.nonNull(taskDefinition)) {
            taskModel.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
            taskModel.setRateLimitFrequencyInSeconds(
                    taskDefinition.getRateLimitFrequencyInSeconds());
            taskModel.setIsolationGroupId(taskDefinition.getIsolationGroupId());
            taskModel.setExecutionNameSpace(taskDefinition.getExecutionNameSpace());
        }

        return List.of(taskModel);
    }
}

