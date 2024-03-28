package com.veeva.vault.custom.processors;

import com.veeva.vault.sdk.api.core.*;
import com.veeva.vault.sdk.api.csv.CsvBuilder;
import com.veeva.vault.sdk.api.csv.CsvData;
import com.veeva.vault.sdk.api.csv.CsvService;
import com.veeva.vault.sdk.api.csv.CsvWriteParameters;
import com.veeva.vault.sdk.api.http.*;
import com.veeva.vault.sdk.api.job.*;
import com.veeva.vault.sdk.api.json.JsonArray;
import com.veeva.vault.sdk.api.json.JsonData;
import com.veeva.vault.sdk.api.json.JsonObject;
import com.veeva.vault.sdk.api.json.JsonValueType;
import com.veeva.vault.sdk.api.notification.NotificationMessage;
import com.veeva.vault.sdk.api.notification.NotificationParameters;
import com.veeva.vault.sdk.api.notification.NotificationService;
import com.veeva.vault.sdk.api.query.Query;
import com.veeva.vault.sdk.api.query.QueryExecutionRequest;
import com.veeva.vault.sdk.api.query.QueryLogicalOperator;
import com.veeva.vault.sdk.api.query.QueryService;
import org.omg.PortableInterceptor.USER_EXCEPTION;

import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Job to Delete Customer Data
 */

@JobInfo(adminConfigurable = true)
public class CustomerDocs implements Job {

    //TODO: ADD job logger info

    @Override
    public JobInputSupplier init(JobInitContext jobInitContext) {

        @SuppressWarnings("unchecked")
        List<JobItem> jobItems = VaultCollections.newList();

        QueryService queryService = ServiceLocator.locate(QueryService.class);

        String queryDate = greaterThanSixtyDays();

        // Testing doc creation date
        Query myQuery= queryService.newQueryBuilder()
                .withSelect(VaultCollections.asList("id"))
                .withFrom("documents")
                .withWhere("customer_data__c = 'TRUE'")
                .appendWhere(QueryLogicalOperator.AND, String.format("document_creation_date__v <= '%s'", queryDate))
                .build();

        QueryExecutionRequest queryExecutionRequest = queryService
                .newQueryExecutionRequestBuilder()
                .withQuery(myQuery)
                .build();


        queryService.query(queryExecutionRequest)
                .onSuccess(
                        queryExecutionResponse -> {
                            queryExecutionResponse.streamResults().forEach(
                                    queryExecutionResult -> {
                                        String docId = queryExecutionResult
                                                .getValue("id", ValueType.STRING);

                                        JobItem jobItem = addJobItem(jobItems, jobInitContext);
                                        jobItem.setValue("id", docId);

                                    }
                            );
                        }
                ).onError(
                        queryOperationError -> {
                            queryOperationError.getQueryOperationErrorType();
                        }
                ).execute();


        return jobInitContext.newJobInput(jobItems);
    }



    @Override
    public void process(JobProcessContext jobProcessContext) {
        List<JobItem> docIds = jobProcessContext.getCurrentTask().getItems();

        LogService logService = ServiceLocator.locate(LogService.class);

        // Create csv

        CsvService csvService = ServiceLocator.locate(CsvService.class);

        CsvWriteParameters parameters = csvService.newCsvWriteParameters()
                .setHeaders(VaultCollections.asList("id"));

        CsvBuilder csvBuilder = csvService.newCsvBuilder(parameters);




        for (JobItem doc : docIds) {
            csvBuilder.addRow(VaultCollections.asList(
                    doc.getValue("id", JobValueType.STRING)
            ));;

        }

        CsvData csvData = csvBuilder.build();
        String csvString = csvData.asString();


        // HTTP Service - POST BATCH DELETE and QUERY REQUEST
        HttpService httpService = ServiceLocator.locate(HttpService.class);

        // Notification - Send to Creators of Documents failing to Delete
        // Cause - Either Blocking References or Failure Other Reason
        NotificationService notificationService = ServiceLocator.locate(NotificationService.class);


        // Build HTTP request

        HttpRequest httpRequest = httpService.newHttpRequest("local_connection");

        httpRequest.setMethod(HttpMethod.DELETE);
        httpRequest.setContentType(HttpRequestContentType.TEXT_CSV);
        httpRequest.appendPath("/api/v23.3/objects/documents/batch");
        httpRequest.setBody(csvString);



        httpService.send(httpRequest, HttpResponseBodyValueType.JSONDATA)
                .onSuccess(response -> {
                    JsonData jsonResponse = response.getResponseBody();

                    if (jsonResponse.isValidJson()) {
                        String responseStatus = jsonResponse.getJsonObject().getValue("responseStatus", JsonValueType.STRING);

                        if (responseStatus.equals("SUCCESS")) {
                            int responseCode = response.getHttpStatusCode();
                            logService.info("RESPONSE: " + responseCode);

                            JsonArray data = jsonResponse.getJsonObject().getValue("data", JsonValueType.ARRAY);

                            List<String> docs = VaultCollections.newList();
                            for (int i = 0; i < data.getSize(); i++) {

                                JsonObject jsonObject = data.getValue(i, JsonValueType.OBJECT);
                                if (jsonObject.getValue("responseStatus", JsonValueType.STRING).equals("FAILURE")) {
                                    BigDecimal id = jsonObject.getValue("id", JsonValueType.NUMBER);
                                    docs.add(id.toString());


                                }

                            }

                            if (docs.size() > 0) {
                                // Create query
                                String failedIdQuery = "SELECT created_by__v, id FROM documents WHERE id CONTAINS('" +
                                        String.join("','", docs) + "')";

                                logService.info(failedIdQuery);
                                HttpRequest queryRequest = httpService.newHttpRequest("local_connection");
                                queryRequest.setMethod(HttpMethod.POST);
                                queryRequest.appendPath("/api/v23.3/query");
                                queryRequest.setBodyParam("q", failedIdQuery);
                                httpService.send(queryRequest, HttpResponseBodyValueType.JSONDATA)
                                        .onSuccess(queryResponse -> {
                                            int queryResponseCode = queryResponse.getHttpStatusCode();
                                            logService.info("Query RESPONSE: " + queryResponseCode);

                                            JsonData queryJsonData = queryResponse.getResponseBody();

                                            JsonArray queryData = queryJsonData.getJsonObject().getValue("data", JsonValueType.ARRAY);

                                            Map<String, List<String>> userDocMap = VaultCollections.newMap();

                                            for (int item = 0; item < queryData.getSize(); item++) {
                                                JsonObject object = queryData.getValue(item, JsonValueType.OBJECT);
                                                String iD = object.getValue("id", JsonValueType.NUMBER).toString();
                                                String userId = object.getValue("created_by__v", JsonValueType.NUMBER).toString();
                                                if (userDocMap.get(userId) == (null)) {
                                                    userDocMap.put(userId, VaultCollections.newList());
                                                    userDocMap.get(userId).add(iD);
                                                } else {
                                                    userDocMap.get(userId).add(iD);
                                                }
                                            }


                                            for (String idUser:
                                                 userDocMap.keySet()) {

                                                List<String> docList = userDocMap.get(idUser);
                                                for (int i = 0; i < docList.size(); i++) {
                                                    docList.set(i, "<li>${uiBaseExtUrl}/#doc_info/" + docList.get(i) + "</li>");
                                                }

                                                String notificationText = "Hello ${recipientFirstName},<br>" +
                                                        "<br>This is to inform you the following document(s) needs to be deleted due to Compliance in ${vaultName}:<br><br><ul>" +
                                                        String.join("",userDocMap.get(idUser)) + "</ul><br>" +
                                                        "<br><br>Thank you,<br>Support Compliance";

                                                NotificationMessage message = notificationService.newNotificationMessage()
                                                        .setMessage(notificationText)
                                                        .setNotificationText(notificationText)
                                                        .setSubject("${vaultId} Data Compliance: Document Deletion REQUIRED");

                                                Set<String> set = VaultCollections.newSet();
                                                set.add(idUser);

                                                NotificationParameters notificationParameters = notificationService.newNotificationParameters()
                                                        .setRecipientsByUserIds(set);

                                                notificationService.send(notificationParameters, message);
                                            }




                                        })
                                        .onError(httpOperationError -> {
                                            int queryResponseCode = httpOperationError.getHttpResponse().getHttpStatusCode();
                                            logService.info("Query RESPONSE: " + queryResponseCode);
                                            logService.info(httpOperationError.getMessage());
                                            logService.info(httpOperationError.getHttpResponse().getResponseBody());
                                        }

                                        ).execute();
                            }


                        }
                        else {
                            logService.info(String.format("Response status: %s", responseStatus));
                            logService.info("Size of document ID list: " + docIds.size());
                        }


                    }

                    else {
                        logService.info("HTTP Response: Invalid JSON Response received.");
                    }

                })
                .onError(httpOperationError -> {
                    int responseCode = httpOperationError.getHttpResponse().getHttpStatusCode();
                    logService.info("RESPONSE: " + responseCode);
                    logService.info(httpOperationError.getMessage());
                    logService.info(httpOperationError.getHttpResponse().getResponseBody());
                })
                .execute();


    }

    @Override
    public void completeWithSuccess(JobCompletionContext jobCompletionContext) {
        JobLogger logger = jobCompletionContext.getJobLogger();
        logger.log("All tasks completed successfully");
    }

    @Override
    public void completeWithError(JobCompletionContext jobCompletionContext) {
        JobResult result = jobCompletionContext.getJobResult();

        JobLogger logger = jobCompletionContext.getJobLogger();
        logger.log("completeWithError: " + result.getNumberFailedTasks() + "tasks failed out of " + result.getNumberTasks());

        List<JobTask> tasks = jobCompletionContext.getTasks();
        for (JobTask task : tasks) {
            TaskOutput taskOutput = task.getTaskOutput();
            if (TaskState.ERRORS_ENCOUNTERED.equals(taskOutput.getState())) {
                logger.log(task.getTaskId() + " failed with error message " + taskOutput.getValue("firstError", JobValueType.STRING));
            }
        }
    }

    public JobItem addJobItem(List<JobItem> jobItems, JobInitContext jobInitContext) {
        JobItem jobItem = jobInitContext.newJobItem();
        jobItems.add(jobItem);
        return jobItem;
    }

    public String greaterThanSixtyDays() {
//        final int DAYSMAX = 60;
        final int DAYSMAX = 30;
        String date = ZonedDateTime.now().minusDays(DAYSMAX).format(DateTimeFormatter.ISO_INSTANT);
        String queryDate = date.substring(0, date.indexOf(".")) + ".000Z";
        return queryDate;
    }


}
