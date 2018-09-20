/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.service.bigquery;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.gcp.GCPUtils;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;


public class BigQueryService extends AbstractWranglerService {
  private static final String DATASET_ID = "datasetId";
  private static final String TABLE_ID = "id";
  private static final String SCHEMA = "schema";
  private static final String BUCKET = "bucket";

  /**
   * Tests BigQuery Connection.
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @POST
  @Path("/connections/bigquery/test")
  public void testBiqQueryConnection(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
      ConnectionType connectionType = ConnectionType.fromString(connection.getType().getType());
      if (connectionType == ConnectionType.UNDEFINED || connectionType != ConnectionType.BIGQUERY) {
        error(responder,
              String.format("Invalid connection type %s set, expected 'bigquery' connection type.",
                            connectionType.getType()));
        return;
      }

      BigQuery bigQuery = GCPUtils.getBigQueryService(connection);
      bigQuery.listDatasets(BigQuery.DatasetListOption.pageSize(1));
      ServiceUtils.success(responder, "Success");
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * List all Datasets.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("connections/{connection-id}/bigquery")
  public void listDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("connection-id") String connectionId) throws Exception {
    Connection connection = store.get(connectionId);

    if (!validateConnection(connectionId, connection, responder)) {
      return;
    }
    BigQuery bigQuery = GCPUtils.getBigQueryService(connection);
    Page<Dataset> datasets = bigQuery.listDatasets(BigQuery.DatasetListOption.all());
    JsonArray values = new JsonArray();
    for (Dataset dataset : datasets.iterateAll()) {
      JsonObject object =  new JsonObject();
      object.addProperty("name", dataset.getDatasetId().getDataset());
      object.addProperty("created", dataset.getCreationTime());
      object.addProperty("description", dataset.getDescription());
      object.addProperty("last-modified", dataset.getLastModified());
      object.addProperty("location", dataset.getLocation());
      values.add(object);
    }
    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", values.size());
    response.add("values", values);
    sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
  }

  /**
   * List all Datasets.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("connections/{connection-id}/bigquery/{dataset-id}/tables")
  public void listTables(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("connection-id") String connectionId,
                           @PathParam("dataset-id") String datasetId) throws Exception {
    Connection connection = store.get(connectionId);

    if (!validateConnection(connectionId, connection, responder)) {
      return;
    }
    BigQuery bigQuery = GCPUtils.getBigQueryService(connection);
    try {
      Page<com.google.cloud.bigquery.Table> tablePage = bigQuery.listTables(datasetId);
      JsonArray values = new JsonArray();

      for (com.google.cloud.bigquery.Table table : tablePage.iterateAll()) {
        JsonObject object = new JsonObject();

        object.addProperty("name", table.getFriendlyName());
        object.addProperty(TABLE_ID, table.getTableId().getTable());
        object.addProperty("created", table.getCreationTime());
        object.addProperty("description", table.getDescription());
        object.addProperty("last-modified", table.getLastModifiedTime());
        object.addProperty("expiration-time", table.getExpirationTime());
        object.addProperty("etag", table.getEtag());

        values.add(object);
      }

      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (BigQueryException e) {
      if (e.getReason() != null) {
        // CDAP-14155 - BigQueryException message is too large. Instead just throw reason of the exception
        throw new RuntimeException(e.getReason());
      }
      // Its possible that reason of the BigQueryException is null, in that case use exception message
      throw new RuntimeException(e.getMessage());
    }
  }

  /**
   * List all Datasets.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param connectionId Connection Id for BigQuery Service.
   * @param datasetId id of the dataset on BigQuery.
   * @param tableId id of the BigQuery table.
   * @param scope Group the workspace is created in.
   */
  @GET
  @Path("connections/{connection-id}/bigquery/{dataset-id}/tables/{table-id}/read")
  public void readTable(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("connection-id") String connectionId,
                           @PathParam("dataset-id") String datasetId,
                           @PathParam("table-id") String tableId,
                           @QueryParam("scope") String scope) throws Exception {
    Connection connection = store.get(connectionId);

    if (!validateConnection(connectionId, connection, responder)) {
      return;
    }

    if (scope == null || scope.isEmpty()) {
      scope = WorkspaceDataset.DEFAULT_SCOPE;
    }

    Map<String, String> connectionProperties = connection.getAllProps();
    String projectId = connectionProperties.get(GCPUtils.PROJECT_ID);
    String path = connectionProperties.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE);
    String bucket = connectionProperties.get(BUCKET);
    TableId tableIdObject =
      projectId == null ? TableId.of(datasetId, tableId) : TableId.of(projectId, datasetId, tableId);
    Pair<List<Row>, Schema> tableData = getData(connection, tableIdObject);

    String identifier = ServiceUtils.generateMD5(String.format("%s:%s", scope, tableId));
    ws.createWorkspaceMeta(identifier, scope, tableId);
    ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
    byte[] data = serDe.toByteArray(tableData.getFirst());
    ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, DataType.RECORDS, data);

    Map<String, String> properties = new HashMap<>();
    properties.put(PropertyIds.ID, identifier);
    properties.put(PropertyIds.NAME, tableId);
    properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.BIGQUERY.getType());
    properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
    properties.put(PropertyIds.CONNECTION_ID, connectionId);
    properties.put(TABLE_ID, tableId);
    properties.put(DATASET_ID, datasetId);
    properties.put(GCPUtils.PROJECT_ID, projectId);
    properties.put(GCPUtils.SERVICE_ACCOUNT_KEYFILE, path);
    properties.put(SCHEMA, tableData.getSecond().toString());
    properties.put(BUCKET, bucket);

    ws.writeProperties(identifier, properties);

    JsonArray values = new JsonArray();
    JsonObject object = new JsonObject();
    object.addProperty(PropertyIds.ID, identifier);
    object.addProperty(PropertyIds.NAME, tableId);
    object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
    values.add(object);
    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", values.size());
    response.add("values", values);
    sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @Path("/connections/{connection-id}/bigquery/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("connection-id") String connectionId,
                            @QueryParam("wid") String workspaceId) {
    JsonObject response = new JsonObject();
    try {
      Map<String, String> config = ws.getProperties(workspaceId);

      Map<String, String> properties = new HashMap<>();
      JsonObject value = new JsonObject();

      JsonObject bigQuery = new JsonObject();
      properties.put("serviceFilePath", config.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
      properties.put("bucket", config.get(BUCKET));
      properties.put("project", config.get(GCPUtils.PROJECT_ID));
      properties.put("dataset", config.get(DATASET_ID));
      properties.put("table", config.get(TABLE_ID));
      properties.put("schema", config.get(SCHEMA));
      bigQuery.add("properties", new Gson().toJsonTree(properties));
      bigQuery.addProperty("name", "BigQueryTable");
      bigQuery.addProperty("type", "source");
      value.add("BigQueryTable", bigQuery);

      JsonArray values = new JsonArray();
      values.add(value);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());

    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  private Pair<List<Row>, Schema> getData(Connection connection, TableId tableId) throws Exception {
    List<Row> rows = new ArrayList<>();
    BigQuery bigQuery = GCPUtils.getBigQueryService(connection);
    String tableIdString =
      tableId.getProject() == null ? String.format("%s.%s", tableId.getDataset(), tableId.getTable()) :
        String.format("%s.%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());
    String query = String.format("SELECT * FROM `%s` LIMIT 1000", tableIdString);
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the job to finish
    queryJob = queryJob.waitFor();

    // check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists.");
    } else if (queryJob.getStatus().getError() != null) {
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results
    QueryResult result = queryJob.getQueryResults().getResult();
    com.google.cloud.bigquery.Schema schema = result.getSchema();
    FieldList fields = schema.getFields();
    for (FieldValueList fieldValues : result.iterateAll()) {
      Row row = new Row();
      for (Field field : fields) {
        String fieldName = field.getName();
        FieldValue fieldValue = fieldValues.get(fieldName);

        LegacySQLTypeName type = field.getType();
        StandardSQLTypeName standardType = type.getStandardType();
        if (fieldValue.isNull()) {
          row.add(fieldName, null);
          continue;
        }
        switch (standardType) {
          case TIME:
            row.add(fieldName, LocalTime.parse(fieldValue.getStringValue()));
            break;

          case DATE:
            row.add(fieldName, LocalDate.parse(fieldValue.getStringValue()));
            break;

          case DATETIME:
          case TIMESTAMP:
            long tsMicroValue = fieldValue.getTimestampValue();
            row.add(fieldName, getZonedDateTime(tsMicroValue));
            break;

          case STRING:
            row.add(fieldName, fieldValue.getStringValue());
            break;

          case BOOL:
            row.add(fieldName, fieldValue.getBooleanValue());
            break;

          case FLOAT64:
            row.add(fieldName, fieldValue.getDoubleValue());
            break;

          case INT64:
            row.add(fieldName, fieldValue.getLongValue());
            break;

          case BYTES:
            row.add(fieldName, fieldValue.getBytesValue());
            break;
        }
      }

      rows.add(row);
    }

    List<Schema.Field> schemaFields = new ArrayList<>();
    for (Field field : fields) {
      LegacySQLTypeName type = field.getType();
      StandardSQLTypeName standardType = type.getStandardType();
      Schema schemaType = null;
      switch (standardType) {
        case BOOL:
          schemaType = Schema.of(Schema.Type.BOOLEAN);
          break;
        case DATE:
          schemaType = Schema.of(Schema.LogicalType.DATE);
          break;
        case TIME:
          schemaType = Schema.of(Schema.LogicalType.TIME_MICROS);
          break;
        case DATETIME:
        case TIMESTAMP:
          schemaType = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
          break;
        case BYTES:
          schemaType = Schema.of(Schema.Type.BYTES);
          break;
        case INT64:
          schemaType = Schema.of(Schema.Type.LONG);
          break;
        case STRING:
          schemaType = Schema.of(Schema.Type.STRING);
          break;
        case FLOAT64:
          schemaType = Schema.of(Schema.Type.DOUBLE);
          break;
      }

      if (schemaType == null) {
        continue;
      }

      String name = field.getName();
      Schema.Field schemaField;
      if (field.getMode() == null || field.getMode() == Field.Mode.NULLABLE) {
        Schema fieldSchema = Schema.nullableOf(schemaType);
        schemaField = Schema.Field.of(name, fieldSchema);
      } else {
        schemaField = Schema.Field.of(name, schemaType);
      }
      schemaFields.add(schemaField);
    }
    Schema schemaToReturn = Schema.recordOf("bigquerySchema", schemaFields);
    return new Pair<>(rows, schemaToReturn);
  }

  private ZonedDateTime getZonedDateTime(long microTs) {
    long tsInSeconds = TimeUnit.MICROSECONDS.toSeconds(microTs);
    long mod = TimeUnit.MICROSECONDS.convert(1, TimeUnit.SECONDS);
    int fraction = (int) (microTs % mod);
    Instant instant = Instant.ofEpochSecond(tsInSeconds, TimeUnit.MICROSECONDS.toNanos(fraction));
    return ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC));
  }

  private boolean validateConnection(String connectionId, Connection connection,
                                     HttpServiceResponder responder) {
    if (connection == null) {
      error(responder, "Unable to find connection in store for the connection id - " + connectionId);
      return false;
    }
    if (ConnectionType.BIGQUERY != connection.getType()) {
      error(responder, "Invalid connection type set, this endpoint only accepts BIGQUERY connection type");
      return false;
    }
    return true;
  }
}
