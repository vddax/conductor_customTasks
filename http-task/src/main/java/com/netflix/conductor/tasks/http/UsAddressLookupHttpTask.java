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
 * Server-side system task for US Address Lookup. Python parity with {@code USAddressLookup.py}.
 *
 * <p>Expected {@code http_request.body} shape:
 *
 * <ul>
 *   <li>{@code header}: Map (e.g. contains jobId)
 *   <li>{@code request}: canonical_records (List of records each containing input.firm/address1/address2/lastline)
 * </ul>
 *
 * <p>Outputs:
 *
 * <ul>
 *   <li>{@code response.body.canonical_records}: enriched canonical records (Python parity)
 *   <li>{@code response}: Standard HTTP response map from {@link HttpTask}
 * </ul>
 */
@Component(UsAddressLookupHttpTask.TASK_TYPE)
public class UsAddressLookupHttpTask extends HttpTask {

    public static final String TASK_TYPE = "US_ADDRESS_LOOKUP";

    @SuppressWarnings("unused")
    public UsAddressLookupHttpTask(
            RestTemplateProvider restTemplateProvider, ObjectMapper objectMapper) {
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
        List<Map<String, Object>> canonicalRecords =
                objectMapper.convertValue(body.get("request"), List.class);
        if (canonicalRecords == null) {
            canonicalRecords = new ArrayList<>();
        }
        if (header == null) {
            header = new HashMap<>();
        }

        List<Map<String, Object>> requestsPayload = new ArrayList<>();
        Map<Integer, Integer> recordIndexMap = new HashMap<>();
        int apiIndex = 0;

        for (int recordIdx = 0; recordIdx < canonicalRecords.size(); recordIdx++) {
            Map<String, Object> rec = canonicalRecords.get(recordIdx);
            try {
                Map<String, Object> addressRecord = new HashMap<>();
                addressRecord.put("firm", getNested(rec, "input", "firm"));
                addressRecord.put("address1", getNested(rec, "input", "address1"));
                addressRecord.put("address2", getNested(rec, "input", "address2"));
                addressRecord.put("lastline", getNested(rec, "input", "lastline"));

                // Python uses KeyError; in Java treat missing as failure if any required field is null
                if (addressRecord.get("address1") == null || addressRecord.get("lastline") == null) {
                    markFailed(
                            rec,
                            "us_address_lookup",
                            "Address or lastline is missing in input",
                            "US_ADDRESS_LOOKUP_FAILED");
                    continue;
                }

                requestsPayload.add(addressRecord);
                recordIndexMap.put(apiIndex, recordIdx);
                apiIndex++;
            } catch (Exception e) {
                markFailed(
                        rec,
                        "us_address_lookup",
                        "Address or lastline is missing in input",
                        "US_ADDRESS_LOOKUP_FAILED");
            }
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

        try {
            HttpResponse response = httpCall(input);

            List<Map<String, Object>> apiResults = normalizeResults(response.body);
            if (apiResults == null) {
                for (Integer idx : recordIndexMap.values()) {
                    if (idx != null && idx >= 0 && idx < canonicalRecords.size()) {
                        markFailed(
                                canonicalRecords.get(idx),
                                "us_address_lookup",
                                "Unexpected API response format",
                                "US_ADDRESS_LOOKUP_FAILED");
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
                        svc.put("status", "SUCCESS");
                        svc.put("output", result.get("output"));
                        services.put("us_address_lookup", svc);

                        rec.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
                        ((Map<String, Object>) rec.get("meta"))
                                .put("status", "US_ADDRESS_LOOKUP_COMPLETED");
                    } else {
                        Map<String, Object> svc = new HashMap<>();
                        svc.put("status", "FAILED");
                        svc.put("error", result.getOrDefault("error", "Unknown failure"));
                        services.put("us_address_lookup", svc);

                        rec.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
                        ((Map<String, Object>) rec.get("meta"))
                                .put("status", "US_ADDRESS_LOOKUP_FAILED");
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
    private static Object getNested(Map<String, Object> root, String... path) {
        Object current = root;
        for (String p : path) {
            if (!(current instanceof Map)) {
                return null;
            }
            current = ((Map<String, Object>) current).get(p);
        }
        return current;
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
    private List<Map<String, Object>> normalizeResults(Object responseBody) {
        if (responseBody == null) {
            return null;
        }
        if (responseBody instanceof List) {
            return (List<Map<String, Object>>) responseBody;
        }
        if (responseBody instanceof Map) {
            return List.of((Map<String, Object>) responseBody);
        }
        return null;
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

