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
package com.netflix.conductor.tasks.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.netflix.conductor.core.execution.WorkflowExecutor;
import com.netflix.conductor.core.utils.Utils;
import com.netflix.conductor.model.TaskModel;
import com.netflix.conductor.model.WorkflowModel;
import com.netflix.conductor.tasks.http.providers.RestTemplateProvider;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A no-worker (server-side) system task that extends {@link HttpTask} and performs request/response
 * pre/post-processing for the "nameparse" service.
 *
 * <p>This task is designed to work with workflows that provide the request payload inside {@code
 * http_request.body}.
 *
 * <p>Expected {@code http_request.body} shape:
 *
 * <ul>
 *   <li>{@code header}: Map (e.g. contains jobId)
 *   <li>{@code request}: canonical_records (List of records each containing {@code input.name})
 *   <li>{@code config} (optional): Map (e.g. nameType/nameOrder/delimiter)
 * </ul>
 *
 * <p>Outputs:
 *
 * <ul>
 *   <li>{@code response.body.canonical_records}: enriched canonical records (Python parity)
 *   <li>{@code response}: Standard HTTP response map from {@link HttpTask}
 * </ul>
 */
@Component(NameparseHttpTask.TASK_TYPE)
public class NameparseHttpTask extends HttpTask {

    public static final String TASK_TYPE = "NAMEPARSE";

    @SuppressWarnings("unused")
    public NameparseHttpTask(RestTemplateProvider restTemplateProvider, ObjectMapper objectMapper) {
        super(TASK_TYPE, restTemplateProvider, objectMapper);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start(WorkflowModel workflow, TaskModel task, WorkflowExecutor executor) {
        Object request = task.getInputData().get(REQUEST_PARAMETER_NAME);
        task.setWorkerId(Utils.getServerId());
        if (request == null) {
            task.setReasonForIncompletion(MISSING_REQUEST);
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        Input input = objectMapper.convertValue(request, Input.class);
        if (input.getUri() == null) {
            task.setReasonForIncompletion(
                    "Missing HTTP URI.  See documentation for HttpTask for required input parameters");
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }
        if (input.getMethod() == null) {
            task.setReasonForIncompletion("No HTTP method specified");
            task.setStatus(TaskModel.Status.FAILED);
            return;
        }

        // ---- Python parity: pre-processing ----
        Object bodyObj = input.getBody();
        if (!(bodyObj instanceof Map)) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("http_request.body must be a JSON object");
            return;
        }
        Map<String, Object> body = (Map<String, Object>) bodyObj;
        if (!body.containsKey("header")) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion("http_request.body.header is required");
            return;
        }
        if (!body.containsKey("request")) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(
                    "http_request.body.request (canonical_records) is required");
            return;
        }

        Map<String, Object> header = objectMapper.convertValue(body.get("header"), Map.class);
        Map<String, Object> config = objectMapper.convertValue(body.get("config"), Map.class);
        List<Map<String, Object>> canonicalRecords =
                objectMapper.convertValue(body.get("request"), List.class);

        if (canonicalRecords == null) {
            canonicalRecords = new ArrayList<>();
        }
        if (header == null) {
            header = new HashMap<>();
        }

        Map<String, Object> mergedConfig = new HashMap<>();
        mergedConfig.put("nameType", "M");
        mergedConfig.put("nameOrder", "first-name-first");
        mergedConfig.put("delimiter", ",");
        if (config != null) {
            mergedConfig.putAll(config);
        }

        List<Map<String, Object>> requestsPayload = new ArrayList<>();
        Map<Integer, Integer> recordIndexMap = new HashMap<>();
        int apiIndex = 0;

        for (int recordIdx = 0; recordIdx < canonicalRecords.size(); recordIdx++) {
            Map<String, Object> rec = canonicalRecords.get(recordIdx);
            Object name = getNested(rec, "input", "name");
            if (name == null) {
                markFailed(rec, "nameparse", "Name Missing in Input", "NAMEPARSE_FAILED");
                continue;
            }
            Map<String, Object> nameRecord = new HashMap<>();
            nameRecord.put("name", name);
            nameRecord.putAll(mergedConfig);
            requestsPayload.add(nameRecord);
            recordIndexMap.put(apiIndex, recordIdx);
            apiIndex++;
        }

        if (requestsPayload.isEmpty()) {
            HttpResponse response = new HttpResponse();
            response.statusCode = 200;
            response.body = wrapBodyWithCanonicalRecords(null, canonicalRecords);
            task.setStatus(TaskModel.Status.COMPLETED);
            task.addOutput("response", response.asMap());
            return;
        }

        Map<String, Object> outboundPayload = new HashMap<>();
        outboundPayload.put("header", header);
        if (requestsPayload.size() == 1) {
            outboundPayload.put("request", requestsPayload.get(0));
        } else {
            outboundPayload.put("requests", requestsPayload);
        }
        input.setBody(outboundPayload);

        // ---- Execute HTTP call ----
        try {
            HttpResponse response = httpCall(input);

            // ---- Python parity: post-processing ----
            List<Map<String, Object>> apiResults = normalizeResults(response.body);
            if (apiResults == null) {
                for (Integer idx : recordIndexMap.values()) {
                    if (idx != null && idx >= 0 && idx < canonicalRecords.size()) {
                        markFailed(
                                canonicalRecords.get(idx),
                                "nameparse",
                                "Unexpected API response format",
                                "NAMEPARSE_FAILED");
                    }
                }
            } else {
                if (apiResults.size() != recordIndexMap.size()) {
                    task.setStatus(TaskModel.Status.FAILED);
                    task.setReasonForIncompletion("External API response does not match request");
                    return;
                }
                for (int i = 0; i < apiResults.size(); i++) {
                    Integer recordIdx = recordIndexMap.get(i);
                    if (recordIdx == null
                            || recordIdx < 0
                            || recordIdx >= canonicalRecords.size()) {
                        continue;
                    }
                    Map<String, Object> rec = canonicalRecords.get(recordIdx);
                    Map<String, Object> result = apiResults.get(i);

                    rec.computeIfAbsent("services", __ -> new HashMap<String, Object>());
                    Map<String, Object> services = (Map<String, Object>) rec.get("services");

                    if (result.containsKey("output")) {
                        Map<String, Object> svc = new HashMap<>();
                        svc.put("status", "success");
                        svc.put("name", result.get("name"));
                        svc.put("nameType", result.get("nameType"));
                        svc.put("nameOrder", result.get("nameOrder"));
                        svc.put("delimiter", result.get("delimiter"));
                        svc.put("output", result.get("output"));
                        svc.put("appendage", result.get("appendage"));
                        services.put("nameparse", svc);

                        rec.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
                        ((Map<String, Object>) rec.get("meta"))
                                .put("status", "NAMEPARSE_COMPLETED");
                    } else {
                        Map<String, Object> svc = new HashMap<>();
                        svc.put("status", "failed");
                        svc.put("error", result.getOrDefault("error", "Unknown failure"));
                        services.put("nameparse", svc);

                        rec.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
                        ((Map<String, Object>) rec.get("meta")).put("status", "NAMEPARSE_FAILED");
                    }
                }
            }

            response.body = wrapBodyWithCanonicalRecords(response.body, canonicalRecords);

            if (response.statusCode > 199 && response.statusCode < 300) {
                if (isAsyncComplete(task)) {
                    task.setStatus(TaskModel.Status.IN_PROGRESS);
                } else {
                    task.setStatus(TaskModel.Status.COMPLETED);
                }
            } else {
                task.setReasonForIncompletion(
                        response.body != null
                                ? response.body.toString()
                                : "No response from the remote service");
                task.setStatus(TaskModel.Status.FAILED);
            }

            task.addOutput("response", response.asMap());
        } catch (Exception e) {
            task.setStatus(TaskModel.Status.FAILED);
            task.setReasonForIncompletion(
                    "Failed to invoke " + getTaskType() + " task due to: " + e);
            task.addOutput("response", e.toString());
        }
    }

    @SuppressWarnings("unchecked")
    private static Object getNested(Map<String, Object> root, String k1, String k2) {
        if (root == null) {
            return null;
        }
        Object lvl1 = root.get(k1);
        if (!(lvl1 instanceof Map)) {
            return null;
        }
        return ((Map<String, Object>) lvl1).get(k2);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> normalizeResults(Object body) {
        if (body == null) {
            return new ArrayList<>();
        }
        if (body instanceof Map) {
            return List.of((Map<String, Object>) body);
        }
        if (body instanceof List) {
            List<Object> raw = (List<Object>) body;
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object o : raw) {
                if (o instanceof Map) {
                    out.add((Map<String, Object>) o);
                } else {
                    return null;
                }
            }
            return out;
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static void markFailed(
            Map<String, Object> record, String serviceKey, String error, String metaStatus) {
        record.computeIfAbsent("services", __ -> new HashMap<String, Object>());
        Map<String, Object> services = (Map<String, Object>) record.get("services");
        Map<String, Object> svc = new HashMap<>();
        svc.put("status", "FAILED");
        svc.put("error", error);
        services.put(serviceKey, svc);

        record.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
        ((Map<String, Object>) record.get("meta")).put("status", metaStatus);
    }

    @SuppressWarnings("unchecked")
    private static Object wrapBodyWithCanonicalRecords(
            Object originalBody, List<Map<String, Object>> canonicalRecords) {
        Map<String, Object> wrapper;
        if (originalBody instanceof Map) {
            wrapper = new HashMap<>((Map<String, Object>) originalBody);
        } else {
            wrapper = new HashMap<>();
            if (originalBody != null) {
                wrapper.put("results", originalBody);
            }
        }
        wrapper.put("canonical_records", canonicalRecords);
        return wrapper;
    }
}
