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
 * Server-side system task for Combined Suppression. Python parity with {@code CompressSupression.py}
 * (combined_suppression).
 *
 * <p>Expected {@code http_request.body} shape:
 *
 * <ul>
 *   <li>{@code header}: Map (e.g. contains jobId)
 *   <li>{@code request}: canonical_records (List of records each containing input fields: name,address1,city,state,postal,email,phone)
 *   <li>{@code combined_suppression_config} (optional): Map with san_id, orientation, nameType, parameters
 * </ul>
 */
@Component(CombinedSuppressionHttpTask.TASK_TYPE)
public class CombinedSuppressionHttpTask extends HttpTask {

    public static final String TASK_TYPE = "COMBINED_SUPPRESSION";

    @SuppressWarnings("unused")
    public CombinedSuppressionHttpTask(
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
        Map<String, Object> cfg =
                objectMapper.convertValue(body.get("combined_suppression_config"), Map.class);

        if (canonicalRecords == null) {
            canonicalRecords = new ArrayList<>();
        }
        if (header == null) {
            header = new HashMap<>();
        }
        if (cfg == null) {
            cfg = new HashMap<>();
        }

        String sanId = cfg.getOrDefault("san_id", "10422243-522243-25").toString();
        String orientation = cfg.getOrDefault("orientation", "FNF").toString();
        String nameType = cfg.getOrDefault("nameType", "M").toString();
        Map<String, Object> parameters = objectMapper.convertValue(cfg.get("parameters"), Map.class);
        if (parameters == null) {
            parameters = new HashMap<>();
        }

        List<Map<String, Object>> requestsPayload = new ArrayList<>();
        Map<Integer, Integer> recordIndexMap = new HashMap<>();
        int apiIndex = 0;

        for (int recordIdx = 0; recordIdx < canonicalRecords.size(); recordIdx++) {
            Map<String, Object> rec = canonicalRecords.get(recordIdx);
            try {
                String fullName = valueOrEmpty(getNested(rec, "input", "name"));
                String firstName = "";
                String lastName = "";
                if (!fullName.isBlank()) {
                    String[] parts = fullName.trim().split("\\s+");
                    if (parts.length > 0) {
                        firstName = parts[0];
                    }
                    if (parts.length > 1) {
                        lastName = parts[1];
                    }
                }

                Map<String, Object> linkage = new HashMap<>();
                linkage.put("dataset", "USERDEF");
                linkage.put("partition", 0);
                linkage.put("recordid", 1452172);

                Map<String, Object> nameEntry = new HashMap<>();
                nameEntry.put("firstName", firstName);
                nameEntry.put("lastName", lastName);

                Map<String, Object> addressEntry = new HashMap<>();
                addressEntry.put(
                        "addressLine",
                        List.of("", valueOrEmpty(getNested(rec, "input", "address1"))));
                addressEntry.put("city", valueOrEmpty(getNested(rec, "input", "city")));
                addressEntry.put("state", valueOrEmpty(getNested(rec, "input", "state")));
                addressEntry.put("zipCode", valueOrEmpty(getNested(rec, "input", "postal")));
                addressEntry.put("zipFour", "");

                Map<String, Object> keys = new HashMap<>();
                keys.put("linkage", linkage);
                keys.put("names", List.of(nameEntry));
                keys.put("address", List.of(addressEntry));
                keys.put("orientation", orientation);
                keys.put("nameType", nameType);

                Map<String, Object> standardKeys = new HashMap<>();
                standardKeys.put("keys", keys);
                standardKeys.put("unparsed", new HashMap<String, Object>());

                Map<String, Object> inputObj = new HashMap<>();
                inputObj.put("standardkeys", Map.of("keys", keys, "unparsed", new HashMap<>()));
                inputObj.put("ftcinput", Map.of("sanid", sanId));
                inputObj.put("email", valueOrEmpty(getNested(rec, "input", "email")));
                inputObj.put("telephone", valueOrEmpty(getNested(rec, "input", "phone")));

                Map<String, Object> suppressionFlags =
                        objectMapper.convertValue(parameters.get("suppressionFlags"), Map.class);
                if (suppressionFlags == null) {
                    suppressionFlags = new HashMap<>();
                }

                Map<String, Object> paramsOut = new HashMap<>();
                paramsOut.put(
                        "suppressionFlags",
                        Map.ofEntries(
                                Map.entry("pandALL", boolFlag(suppressionFlags, "pandALL")),
                                Map.entry("MPSIndicator", boolFlag(suppressionFlags, "MPSIndicator")),
                                Map.entry("DTSIndicator", boolFlag(suppressionFlags, "DTSIndicator")),
                                Map.entry("OfficialIndicator", boolFlag(suppressionFlags, "OfficialIndicator")),
                                Map.entry("BUSIndicator", boolFlag(suppressionFlags, "BUSIndicator")),
                                Map.entry("DMIIndicator", boolFlag(suppressionFlags, "DMIIndicator")),
                                Map.entry("RETIndicator", boolFlag(suppressionFlags, "RETIndicator")),
                                Map.entry("EXTIndicator", boolFlag(suppressionFlags, "EXTIndicator")),
                                Map.entry("COLIndicator", boolFlag(suppressionFlags, "COLIndicator")),
                                Map.entry("MILIndicator", boolFlag(suppressionFlags, "MILIndicator")),
                                Map.entry("TRLIndicator", boolFlag(suppressionFlags, "TRLIndicator")),
                                Map.entry("NURIndicator", boolFlag(suppressionFlags, "NURIndicator")),
                                Map.entry("CLIIndicator", boolFlag(suppressionFlags, "CLIIndicator")),
                                Map.entry("DBAIndicator", boolFlag(suppressionFlags, "DBAIndicator")),
                                Map.entry("ACAIndicator", boolFlag(suppressionFlags, "ACAIndicator")),
                                Map.entry("Reserved", boolFlag(suppressionFlags, "Reserved")),
                                Map.entry("DECIndicator", boolFlag(suppressionFlags, "DECIndicator")),
                                Map.entry("RELIndicator", boolFlag(suppressionFlags, "RELIndicator"))));
                paramsOut.put("performNameAddress", boolFlag(parameters, "performNameAddress"));
                paramsOut.put("performEmail", boolFlag(parameters, "performEmail"));
                paramsOut.put("blankEmails", boolFlag(parameters, "blankEmails"));
                paramsOut.put("performPhone", boolFlag(parameters, "performPhone"));
                paramsOut.put("performFTC", boolFlag(parameters, "performFTC"));
                paramsOut.put("blankFTCPhones", boolFlag(parameters, "blankFTCPhones"));
                paramsOut.put("performAtty", boolFlag(parameters, "performAtty"));
                paramsOut.put("performTPS", boolFlag(parameters, "performTPS"));
                paramsOut.put("performBusinessPhone", boolFlag(parameters, "performBusinessPhone"));

                Map<String, Object> combinedSuppressionRecord = new HashMap<>();
                combinedSuppressionRecord.put("input", inputObj);
                combinedSuppressionRecord.put("parameters", paramsOut);

                requestsPayload.add(combinedSuppressionRecord);
                recordIndexMap.put(apiIndex, recordIdx);
                apiIndex++;
            } catch (Exception e) {
                markFailed(rec, "combined_suppression", "Input is missing in input", "COMBINED_SUPPRESSION_FAILED");
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

            List<Map<String, Object>> apiResults = normalizeResults(response.body, requestsPayload.size());
            if (apiResults == null) {
                for (Integer idx : recordIndexMap.values()) {
                    if (idx != null && idx >= 0 && idx < canonicalRecords.size()) {
                        markFailed(
                                canonicalRecords.get(idx),
                                "combined_suppression",
                                "Unexpected API response format",
                                "COMBINED_SUPPRESSION_FAILED");
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
                        services.put("combined_suppression", svc);

                        rec.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
                        ((Map<String, Object>) rec.get("meta"))
                                .put("status", "COMBINED_SUPPRESSION_COMPLETED");
                    } else {
                        Map<String, Object> svc = new HashMap<>();
                        svc.put("status", "FAILED");
                        svc.put("error", result.getOrDefault("error", "Unknown failure"));
                        services.put("combined_suppression", svc);

                        rec.computeIfAbsent("meta", __ -> new HashMap<String, Object>());
                        ((Map<String, Object>) rec.get("meta"))
                                .put("status", "COMBINED_SUPPRESSION_FAILED");
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

    private static String valueOrEmpty(Object v) {
        return v == null ? "" : v.toString();
    }

    private static boolean boolFlag(Map<String, Object> m, String key) {
        Object v = m.get(key);
        if (v == null) {
            return false;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        return Boolean.parseBoolean(v.toString());
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
    private List<Map<String, Object>> normalizeResults(Object responseBody, int expectedCount) {
        if (responseBody == null) {
            return null;
        }
        if (responseBody instanceof List) {
            return (List<Map<String, Object>>) responseBody;
        }
        if (responseBody instanceof Map) {
            // Python returns a single object for single requests
            if (expectedCount == 1) {
                return List.of((Map<String, Object>) responseBody);
            }
            // For batch, python wraps non-list into a single-element list
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

