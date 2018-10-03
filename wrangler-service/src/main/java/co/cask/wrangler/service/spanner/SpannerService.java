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

package co.cask.wrangler.service.spanner;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.PluginConfiguration;
import co.cask.wrangler.service.ServiceResponse;
import co.cask.wrangler.service.common.AbstractWranglerService;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.service.gcp.GCPUtils;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Spanner data prep connection service
 */
public class SpannerService extends AbstractWranglerService {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerService.class);
  private static final String TABLE_NAME = "TableName";
  // Spanner queries for listing tables and listing schema of table are documented at
  // https://cloud.google.com/spanner/docs/information-schema
  private static final Statement LIST_TABLES_STATEMENT =
    Statement.of("SELECT t.table_name FROM " +
                   "information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = ''");

  private static final Statement.Builder SCHEMA_STATEMENT_BUILDER = Statement.newBuilder(
    String.format("SELECT t.column_name, t.spanner_type, t.is_nullable FROM information_schema.columns AS t WHERE " +
                    "t.table_catalog = '' AND t.table_schema = '' AND t.table_name = @%s", TABLE_NAME));
  // Default Number of rows read from Spanner table by data-prep
  private static final String DEFAULT_ROW_LIMIT = "1000";
  private static final Gson GSON = new Gson();

  /**
   * Tests Spanner Connection.
   */
  @POST
  @Path("/connections/spanner/test")
  public void testSpannerConnection(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent(Charsets.UTF_8.name(), Connection.class);
      GCPUtils.validateProjectCredentials(connection);
      getInstances(connection);
      ServiceUtils.success(responder, "Success");
    } catch (BadRequestException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      ServiceUtils.error(responder, e.getMessage());
    }
  }

  /**
   * Lists spanner instances in the project
   */
  @GET
  @Path("/connections/{connection-id}/spanner/instances")
  public void getSpannerInstances(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("connection-id") final String connectionId) {
    try {
      Connection connection = store.get(connectionId);
      validateConnection(connectionId, connection);
      List<SpannerInstance> instances = getInstances(connection);
      responder.sendJson(new ServiceResponse<>(instances));
    } catch (BadRequestException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  /**
   * Lists spanner databases for a spanner instance
   */
  @GET
  @Path("/connections/{connection-id}/spanner/instances/{instance-id}/databases")
  public void getSpannerDatabases(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("connection-id") final String connectionId,
                                  @PathParam("instance-id") final String instanceId) {
    try {
      Connection connection = store.get(connectionId);
      validateConnection(connectionId, connection);
      List<SpannerDatabase> databases = getDatabases(connection, instanceId);
      responder.sendJson(new ServiceResponse<>(databases));
    } catch (BadRequestException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  /**
   * Lists spanner tables for a spanner database
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @GET
  @Path("/connections/{connection-id}/spanner/instances/{instance-id}/databases/{database-id}/tables")
  public void getSpannerTables(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("connection-id") final String connectionId,
                               @PathParam("instance-id") final String instanceId,
                               @PathParam("database-id") final String databaseId) {
    try {
      Connection connection = store.get(connectionId);
      validateConnection(connectionId, connection);
      List<SpannerTable> tables = getTables(connection, instanceId, databaseId);
      responder.sendJson(new ServiceResponse<>(tables));
    } catch (BadRequestException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  /**
   * Read spanner table into a workspace and return the workspace identifier
   */
  @GET
  @Path("/connections/{connection-id}/spanner/instances/{instance-id}/databases/{database-id}/tables/{table-id}/read")
  public void readTable(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("connection-id") final String connectionId,
                        @PathParam("instance-id") final String instanceId,
                        @PathParam("database-id") final String databaseId,
                        @PathParam("table-id") final String tableId,
                        @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope,
                        @QueryParam("limit") @DefaultValue(DEFAULT_ROW_LIMIT) String limit) {
    try {
      Connection connection = store.get(connectionId);
      validateConnection(connectionId, connection);
      Schema schema = getTableSchema(connection, instanceId, databaseId, tableId);
      List<Row> data = getTableData(connection, instanceId, databaseId, tableId, schema, Long.parseLong(limit));

      // create workspace id
      String identifier = ServiceUtils.generateMD5(String.format("%s:%s", scope, tableId));
      ws.createWorkspaceMeta(identifier, scope, tableId);

      // write data to workspace
      ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
      byte[] dataBytes = serDe.toByteArray(data);
      ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL, DataType.RECORDS, dataBytes);

      Map<String, String> connectionProperties = connection.getAllProps();
      String projectId = connectionProperties.get(GCPUtils.PROJECT_ID);
      String path = connectionProperties.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE);

      String externalDsName = new StringJoiner(".").add(instanceId).add(databaseId).add(tableId).toString();

      SpannerSpecification specification =
        new SpannerSpecification(externalDsName, path, projectId, instanceId, databaseId, tableId, schema);

      // initialize and store workspace properties
      Map<String, String> workspaceProperties = new HashMap<>();
      workspaceProperties.put(PropertyIds.ID, identifier);
      workspaceProperties.put(PropertyIds.NAME, tableId);
      workspaceProperties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.SPANNER.getType());
      workspaceProperties.put(PropertyIds.CONNECTION_ID, connectionId);
      workspaceProperties.put(PropertyIds.PLUGIN_SPECIFICATION, GSON.toJson(specification));

      ws.writeProperties(identifier, workspaceProperties);

      // send the workspace identifier as response
      WorkspaceIdentifier workspaceIdentifier = new WorkspaceIdentifier(identifier, tableId);
      responder.sendJson(new ServiceResponse<>(ImmutableList.of(workspaceIdentifier)));
    } catch (BadRequestException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  /**
   * Get the specification for the spanner source plugin.
   *
   */
  @Path("/spanner/workspaces/{workspace-id}/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("workspace-id") String workspaceId) {
    try {
      Map<String, String> config = ws.getProperties(workspaceId);

      // deserialize and send spanner source specification
      SpannerSpecification specification =
        GSON.fromJson(config.get(PropertyIds.PLUGIN_SPECIFICATION), SpannerSpecification.class);
      PluginConfiguration<SpannerSpecification> pluginConfiguration =
        new PluginConfiguration<>("Spanner", "source", specification);

      // creating map of name -> PluginConfiguration to be consistent with other services response format
      Map<String, PluginConfiguration> pluginMap = ImmutableMap.of("Spanner", pluginConfiguration);
      ServiceResponse<ImmutableMap<String, ImmutableList<PluginConfiguration>>> response =
        new ServiceResponse(ImmutableList.of(pluginMap));
      responder.sendJson(response);
    } catch (JsonSyntaxException | BadRequestException e) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  private Schema getTableSchema(Connection connection,
                                String instanceId, String databaseId, String tableId) throws Exception {
    Spanner spanner = GCPUtils.getSpannerService(connection);
    try {
      String projectId = spanner.getOptions().getProjectId();
      Statement getTableSchemaStatement = SCHEMA_STATEMENT_BUILDER.bind(TABLE_NAME).to(tableId).build();
      try (ResultSet resultSet = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId)).
        singleUse().executeQuery(getTableSchemaStatement)) {
        List<Schema.Field> schemaFields = new ArrayList<>();
        while (resultSet.next()) {
          String columnName = resultSet.getString("column_name");
          String spannerType = resultSet.getString("spanner_type");
          String nullable = resultSet.getString("is_nullable");
          boolean isNullable = "YES".equals(nullable);
          Schema typeSchema = parseSchemaFromSpannerTypeString(spannerType);
          Schema fieldSchema = isNullable ? Schema.nullableOf(typeSchema) : typeSchema;
          schemaFields.add(Schema.Field.of(columnName, fieldSchema));
        }
        return Schema.recordOf("tableSchema", schemaFields);
      }
    } finally {
      spanner.close();
    }
  }

  private Schema parseSchemaFromSpannerTypeString(String spannerType) throws UnsupportedTypeException {
    if (spannerType.startsWith("STRING")) {
      // STRING and BYTES also have size at the end in the format, example : STRING(1024)
      return Schema.of(Schema.Type.STRING);
    } else if (spannerType.startsWith("BYTES")) {
      return Schema.of(Schema.Type.BYTES);
    } else {
      switch (Type.Code.valueOf(spannerType)) {
        case BOOL:
          return Schema.of(Schema.Type.BOOLEAN);
        case INT64:
          return Schema.of(Schema.Type.LONG);
        case FLOAT64:
          return Schema.of(Schema.Type.DOUBLE);
        case DATE:
          return Schema.of(Schema.LogicalType.DATE);
        case TIMESTAMP:
          return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        default:
          throw new UnsupportedTypeException(String.format("Type : %s is unsupported currently", spannerType));
      }
    }
  }

  /**
   * Execute Spanner select query on table with row limit and
   * convert the {@link ResultSet} to {@link Row} and return the list of rows
   */
  private List<Row> getTableData(Connection connection, String instanceId,
                                 String databaseId, String tableId, Schema schema, long limit) throws Exception {
    Spanner spanner = GCPUtils.getSpannerService(connection);
    try {
      String projectId = spanner.getOptions().getProjectId();
      List<String> columnNames = schema.getFields().stream().map(e -> e.getName()).collect(Collectors.toList());
      List<Row> resultRows = new ArrayList<>();
      try (ResultSet resultSet = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId)).singleUse()
        .read(tableId, KeySet.all(), columnNames, Options.limit(limit))) {
        while (resultSet.next()) {
          resultRows.add(convertResultSetToRow(resultSet));
        }
      }
      return resultRows;
    } finally {
      spanner.close();
    }
  }

  private Row convertResultSetToRow(ResultSet resultSet) {
    List<Type.StructField> structFields = resultSet.getType().getStructFields();
    Row row = new Row();
    for (Type.StructField field : structFields) {
      String fieldName = field.getName();
      Type columnType = resultSet.getColumnType(fieldName);
      if (columnType == null || resultSet.isNull(fieldName)) {
        row.add(fieldName, null);
        continue;
      }
      switch (columnType.getCode()) {
        case BOOL:
          row.add(fieldName, resultSet.getBoolean(fieldName));
          break;
        case INT64:
          row.add(fieldName, resultSet.getLong(fieldName));
          break;
        case FLOAT64:
          row.add(fieldName, resultSet.getDouble(fieldName));
          break;
        case STRING:
          row.add(fieldName, resultSet.getString(fieldName));
          break;
        case BYTES:
          ByteArray byteArray = resultSet.getBytes(fieldName);
          row.add(fieldName, byteArray.toByteArray());
          break;
        case DATE:
          // spanner DATE is a date without time zone. so create LocalDate from spanner DATE
          Date spannerDate = resultSet.getDate(fieldName);
          LocalDate date = LocalDate.of(spannerDate.getYear(), spannerDate.getMonth(),
                                        spannerDate.getDayOfMonth());
          row.add(fieldName, date);
          break;
        case TIMESTAMP:
          Timestamp spannerTs = resultSet.getTimestamp(fieldName);
          // Spanner TIMESTAMP supports nano second level precision, however, cdap schema only supports
          // microsecond level precision.
          Instant instant = Instant.ofEpochSecond(spannerTs.getSeconds()).plusNanos(spannerTs.getNanos());
          row.add(fieldName, ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
          break;
      }
    }
    return row;
  }

  private void validateConnection(String inputConnectionId, Connection connection) throws BadRequestException {
    if (connection == null) {
      throw new BadRequestException("Unable to find connection in store for the connection id - " + inputConnectionId);
    }
    if (ConnectionType.SPANNER != connection.getType()) {
      throw new BadRequestException(
        String.format("Invalid connection type %s set, this endpoint only accepts %s",
                      connection.getType(), ConnectionType.SPANNER));
    }
  }

  private List<SpannerInstance> getInstances(Connection connection) throws Exception {
    Spanner spanner = GCPUtils.getSpannerService(connection);
    try {
      List<SpannerInstance> instanceNames = new ArrayList<>();
      spanner.getInstanceAdminClient().listInstances().iterateAll().iterator()
        .forEachRemaining(e -> instanceNames.add(new SpannerInstance(e.getId().getInstance())));
      return instanceNames;
    } finally {
      spanner.close();
    }
  }

  private List<SpannerDatabase> getDatabases(Connection connection, String instanceId) throws Exception {
    Spanner spanner = GCPUtils.getSpannerService(connection);
    try {
      List<SpannerDatabase> databases = new ArrayList<>();
      spanner.getDatabaseAdminClient().listDatabases(instanceId).iterateAll().iterator()
        .forEachRemaining(e -> databases.add(new SpannerDatabase(e.getId().getDatabase())));
      return databases;
    } finally {
      spanner.close();
    }
  }

  private List<SpannerTable> getTables(Connection connection, String instanceId, String databaseId) throws Exception {
    Spanner spanner = GCPUtils.getSpannerService(connection);
    try {
      List<SpannerTable> tables = new ArrayList<>();
      String projectId = spanner.getOptions().getProjectId();
      try (ResultSet resultSet = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId)).
        singleUse().executeQuery(LIST_TABLES_STATEMENT)) {
        while (resultSet.next()) {
          tables.add(new SpannerTable(resultSet.getString("table_name")));
        }
        return tables;
      }
    } finally {
      spanner.close();
    }
  }
}
