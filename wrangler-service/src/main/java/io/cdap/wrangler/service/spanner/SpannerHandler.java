/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.spanner;

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
import com.google.gson.Gson;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.SamplingMethod;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceMeta;
import io.cdap.wrangler.proto.ConnectionSample;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.PluginSpec;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.spanner.SpannerDatabase;
import io.cdap.wrangler.proto.spanner.SpannerSpec;
import io.cdap.wrangler.proto.spanner.SpannerTable;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.gcp.GCPUtils;
import io.cdap.wrangler.utils.ObjectSerDe;
import io.cdap.wrangler.utils.ReferenceNames;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Spanner data prep connection service
 */
@Deprecated
public class SpannerHandler extends AbstractWranglerHandler {
  private static final String PATH_FORMAT = "/%s/%s/%s";
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

  @POST
  @Path("/contexts/{context}/connections/spanner/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void testSpannerConnection(HttpServiceRequest request, HttpServiceResponder responder,
                                    @PathParam("context") String namespace) {
    respond(request, responder, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.SPANNER);
      GCPUtils.validateProjectCredentials(connection);
      getInstances(connection);
      return new ServiceResponse<Void>("Success");
    });
  }

  /**
   * Lists spanner instances in the project
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/spanner/instances")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getSpannerInstances(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("context") String namespace,
                                  @PathParam("connection-id") String connectionId) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.SPANNER);
      List<SpannerInstance> instances = getInstances(connection);
      return new ServiceResponse<>(instances);
    });
  }

  /**
   * Lists spanner databases for a spanner instance
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/spanner/instances/{instance-id}/databases")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getSpannerDatabases(HttpServiceRequest request, HttpServiceResponder responder,
                                  @PathParam("context") String namespace,
                                  @PathParam("connection-id") String connectionId,
                                  @PathParam("instance-id") String instanceId) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.SPANNER);
      List<SpannerDatabase> databases = getDatabases(connection, instanceId);
      return new ServiceResponse<>(databases);
    });
  }

  /**
   * Lists spanner tables for a spanner database
   *
   * @param request HTTP Request handler.
   * @param responder HTTP Response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/spanner/instances/{instance-id}/databases/{database-id}/tables")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getSpannerTables(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace,
                               @PathParam("connection-id") String connectionId,
                               @PathParam("instance-id") String instanceId,
                               @PathParam("database-id") String databaseId) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.SPANNER);
      List<SpannerTable> tables = getTables(connection, instanceId, databaseId);
      return new ServiceResponse<>(tables);
    });
  }

  /**
   * Read spanner table into a workspace and return the workspace identifier
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/spanner/instances/{instance-id}/"
    + "databases/{database-id}/tables/{table-id}/read")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void readTable(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace,
                        @PathParam("connection-id") String connectionId,
                        @PathParam("instance-id") String instanceId,
                        @PathParam("database-id") String databaseId,
                        @PathParam("table-id") String tableId,
                        @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope,
                        @QueryParam("limit") @DefaultValue(DEFAULT_ROW_LIMIT) String limit) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.SPANNER);
      Schema schema = getTableSchema(connection, instanceId, databaseId, tableId);
      List<Row> data = getTableData(connection, instanceId, databaseId, tableId, schema, Long.parseLong(limit));

      Map<String, String> connectionProperties = connection.getProperties();
      String projectId = connectionProperties.get(GCPUtils.PROJECT_ID);
      String path = connectionProperties.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE);

      String externalDsName = new StringJoiner(".").add(instanceId).add(databaseId).add(tableId).toString();

      SpannerSpecification specification =
        new SpannerSpecification(externalDsName, path, projectId, instanceId, databaseId, tableId, schema);

      Map<String, String> workspaceProperties = new HashMap<>();
      workspaceProperties.put(PropertyIds.NAME, tableId);
      workspaceProperties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.SPANNER.getType());
      workspaceProperties.put(PropertyIds.CONNECTION_ID, connectionId);
      workspaceProperties.put(PropertyIds.PLUGIN_SPECIFICATION, GSON.toJson(specification));
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(tableId)
        .setScope(scope)
        .setProperties(workspaceProperties)
        .build();
      String sampleId = TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        NamespacedId workspaceId = ws.createWorkspace(ns, workspaceMeta);

        // write data to workspace
        ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
        byte[] dataBytes = serDe.toByteArray(data);
        ws.updateWorkspaceData(workspaceId, DataType.RECORDS, dataBytes);
        return workspaceId.getId();
      });

      ConnectionSample sample = new ConnectionSample(sampleId, tableId, ConnectionType.SPANNER.getType(),
                                                     SamplingMethod.NONE.getMethod(), connectionId);
      return new ServiceResponse<>(sample);
    });
  }

  /**
   * Get the specification for the spanner source plugin.
   *
   */
  @GET
  @Path("contexts/{context}/spanner/workspaces/{workspace-id}/specification")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("workspace-id") String workspaceId) {
    respond(request, responder, namespace, ns -> {
      Map<String, String> config = getWorkspace(new NamespacedId(ns, workspaceId)).getProperties();

      // deserialize and send spanner source specification
      SpannerSpecification conf =
        GSON.fromJson(config.get(PropertyIds.PLUGIN_SPECIFICATION), SpannerSpecification.class);
      Map<String, String> properties = new HashMap<>();
      properties.put("referenceName", ReferenceNames.cleanseReferenceName(conf.getReferenceName()));
      properties.put("serviceFilePath", conf.getServiceFilePath());
      properties.put("project", conf.getProject());
      properties.put("instance", conf.getInstance());
      properties.put("database", conf.getDatabase());
      properties.put("table", conf.getTable());
      properties.put("schema", conf.getSchema());
      PluginSpec pluginSpec = new PluginSpec("Spanner", "source", properties);
      return new ServiceResponse<>(new SpannerSpec(pluginSpec));
    });
  }

  public static Map<String, String> getConnectorProperties(Map<String, String> config) {
    Map<String, String> properties = new HashMap<>();
    properties.put("serviceAccountType", "filePath");
    properties.put("serviceFilePath", config.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
    properties.put("project", config.get(GCPUtils.PROJECT_ID));
    properties.values().removeIf(Objects::isNull);
    return properties;
  }

  public static String getPath(Workspace workspace) {
    SpannerSpecification conf =
      GSON.fromJson(workspace.getProperties().get(PropertyIds.PLUGIN_SPECIFICATION), SpannerSpecification.class);
    return String.format(PATH_FORMAT, conf.getInstance(), conf.getDatabase(), conf.getTable());
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
      List<String> columnNames = schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
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

  private List<SpannerInstance> getInstances(ConnectionMeta connection) throws Exception {
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

  private List<SpannerDatabase> getDatabases(ConnectionMeta connection, String instanceId) throws Exception {
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
