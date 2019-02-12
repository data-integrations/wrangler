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

package co.cask.wrangler.service.bigquery;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.spi.data.transaction.TransactionRunners;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
import co.cask.wrangler.proto.ConnectionSample;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.PluginSpec;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.bigquery.BigQuerySpec;
import co.cask.wrangler.proto.bigquery.DatasetInfo;
import co.cask.wrangler.proto.bigquery.TableInfo;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.service.gcp.GCPUtils;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
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
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Service for testing, browsing, and reading using a BigQuery connection.
 */
public class BigQueryHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryHandler.class);
  private static final String DATASET_ID = "datasetId";
  private static final String DATASET_PROJECT = "datasetProject";
  private static final String TABLE_ID = "id";
  private static final String SCHEMA = "schema";
  private static final String BUCKET = "bucket";

  @POST
  @Path("/contexts/{context}/connections/bigquery/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void testBiqQueryConnection(HttpServiceRequest request, HttpServiceResponder responder,
                                     @PathParam("context") String namespace) {
    respond(request, responder, () -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.BIGQUERY);
      GCPUtils.validateProjectCredentials(connection);

      BigQuery bigQuery = GCPUtils.getBigQueryService(connection);
      bigQuery.listDatasets(BigQuery.DatasetListOption.pageSize(1));
      return new ServiceResponse<Void>("Success");
    });
  }

  @GET
  @Path("/contexts/{context}/connections/{connection-id}/bigquery")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listDatasets(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace, @PathParam("connection-id") String connectionId) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId), ConnectionType.BIGQUERY);

      BigQuery bigQuery = GCPUtils.getBigQueryService(connection);
      String connectionProject = GCPUtils.getProjectId(connection);
      Set<DatasetId> datasetWhitelist = getDatasetWhitelist(connection);
      List<DatasetInfo> values = new ArrayList<>();
      for (Dataset dataset : getDatasets(bigQuery, datasetWhitelist)) {
        String name = dataset.getDatasetId().getDataset();
        String datasetProject = dataset.getDatasetId().getProject();
        // if the dataset is not in the connection's project, add the <project>: to the front of the name
        if (!connectionProject.equals(datasetProject)) {
          name = new StringJoiner(":").add(datasetProject).add(name).toString();
        }
        values.add(new DatasetInfo(name, dataset.getDescription(), dataset.getLocation(), dataset.getCreationTime(),
                                   dataset.getLastModified()));
      }
      return new ServiceResponse<>(values);
    });
  }

  /**
   * List all tables in a dataset.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param datasetStr the dataset id as a string. It will be of the form [project:]name.
   *   The project prefix is optional. When not given, the connection project should be used.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/bigquery/{dataset-id}/tables")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listTables(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace,
                         @PathParam("connection-id") String connectionId,
                         @PathParam("dataset-id") String datasetStr) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId),
                                                     ConnectionType.BIGQUERY);
      BigQuery bigQuery = GCPUtils.getBigQueryService(connection);

      DatasetId datasetId = getDatasetId(datasetStr, GCPUtils.getProjectId(connection));

      try {
        Page<Table> tablePage = bigQuery.listTables(datasetId);
        List<TableInfo> values = new ArrayList<>();
        for (Table table : tablePage.iterateAll()) {
          values.add(new TableInfo(table.getTableId().getTable(), table.getFriendlyName(), table.getDescription(),
                                   table.getEtag(), table.getCreationTime(), table.getLastModifiedTime(),
                                   table.getExpirationTime()));
        }

        return new ServiceResponse<>(values);
      } catch (BigQueryException e) {
        if (e.getReason() != null) {
          // CDAP-14155 - BigQueryException message is too large. Instead just throw reason of the exception
          throw new RuntimeException(e.getReason());
        }
        throw e;
      }
    });
  }

  /**
   * Read a table.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param connectionId Connection Id for BigQuery Service.
   * @param datasetStr id of the dataset on BigQuery.
   * @param tableId id of the BigQuery table.
   * @param scope Group the workspace is created in.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/bigquery/{dataset-id}/tables/{table-id}/read")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void readTable(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace,
                        @PathParam("connection-id") String connectionId,
                        @PathParam("dataset-id") String datasetStr,
                        @PathParam("table-id") String tableId,
                        @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> {
      Connection connection = getValidatedConnection(new NamespacedId(ns, connectionId),
                                                     ConnectionType.BIGQUERY);

      Map<String, String> connectionProperties = connection.getProperties();
      String connectionProject = GCPUtils.getProjectId(connection);
      DatasetId datasetId = getDatasetId(datasetStr, connectionProject);
      String path = connectionProperties.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE);
      String bucket = connectionProperties.get(BUCKET);
      TableId tableIdObject = TableId.of(datasetId.getProject(), datasetId.getDataset(), tableId);
      Pair<List<Row>, Schema> tableData = getData(connection, tableIdObject);

      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.NAME, tableId);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.BIGQUERY.getType());
      properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      properties.put(PropertyIds.CONNECTION_ID, connectionId);
      properties.put(TABLE_ID, tableId);
      properties.put(DATASET_ID, datasetId.getDataset());
      properties.put(DATASET_PROJECT, datasetId.getProject());
      properties.put(GCPUtils.PROJECT_ID, connectionProject);
      properties.put(GCPUtils.SERVICE_ACCOUNT_KEYFILE, path);
      properties.put(SCHEMA, tableData.getSecond().toString());
      properties.put(BUCKET, bucket);

      String identifier = ServiceUtils.generateMD5(String.format("%s:%s", scope, tableId));
      NamespacedId workspaceId = new NamespacedId(ns, identifier);
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(workspaceId, tableId)
        .setScope(scope)
        .setProperties(properties)
        .build();
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.writeWorkspaceMeta(workspaceMeta);

        ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
        byte[] data = serDe.toByteArray(tableData.getFirst());
        ws.updateWorkspaceData(workspaceId, DataType.RECORDS, data);
      });

      ConnectionSample sample = new ConnectionSample(identifier, tableId, ConnectionType.BIGQUERY.getType(),
                                                     SamplingMethod.NONE.getMethod(), connectionId);
      return new ServiceResponse<>(sample);
    });
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("contexts/{context}/connections/{connection-id}/bigquery/specification")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("connection-id") String connectionId,
                            @QueryParam("wid") String workspaceId) {
    respond(request, responder, namespace, ns -> {
      Map<String, String> config = getWorkspace(new NamespacedId(ns, connectionId)).getProperties();

      Map<String, String> properties = new HashMap<>();
      String externalDatasetName =
        new StringJoiner(".").add(config.get(DATASET_ID)).add(config.get(TABLE_ID)).toString();
      properties.put("referenceName", externalDatasetName);
      properties.put("serviceFilePath", config.get(GCPUtils.SERVICE_ACCOUNT_KEYFILE));
      properties.put("bucket", config.get(BUCKET));
      properties.put("project", config.get(GCPUtils.PROJECT_ID));
      properties.put(DATASET_PROJECT, config.get(DATASET_PROJECT));
      properties.put("dataset", config.get(DATASET_ID));
      properties.put("table", config.get(TABLE_ID));
      properties.put("schema", config.get(SCHEMA));

      PluginSpec pluginSpec = new PluginSpec("BigQueryTable", "source", properties);
      BigQuerySpec spec = new BigQuerySpec(pluginSpec);

      return new ServiceResponse<>(spec);
    });
  }

  private Pair<List<Row>, Schema> getData(Connection connection, TableId tableId)
    throws IOException, InterruptedException {
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

          case TIMESTAMP:
            long tsMicroValue = fieldValue.getTimestampValue();
            row.add(fieldName, getZonedDateTime(tsMicroValue));
            break;

          case DATETIME:
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
        case TIMESTAMP:
          schemaType = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
          break;
        case BYTES:
          schemaType = Schema.of(Schema.Type.BYTES);
          break;
        case INT64:
          schemaType = Schema.of(Schema.Type.LONG);
          break;
        case DATETIME:
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

  /**
   * Parses the dataset whitelist in the connection properties into a set of DatasetId.
   * The whitelist is expected to be a comma separated list of dataset ids, where each dataset id is of the form:
   *
   * [project:]name
   *
   * The project is optional. If it is not given, it is assumed that the dataset is in the connection project.
   *
   * For example, consider the following whitelist:
   *
   * abc:articles,123:d10,d11
   *
   * If the connection is in project 'abc', this will be parsed into 3 dataset ids --
   * [abc,articles], [123,d10], and [abc,d11].
   */
  @VisibleForTesting
  static Set<DatasetId> getDatasetWhitelist(ConnectionMeta connection) {
    String connectionProject = GCPUtils.getProjectId(connection);
    String whitelistStr = connection.getProperties().get("datasetWhitelist");
    Set<DatasetId> whitelist = new LinkedHashSet<>();
    if (whitelistStr == null) {
      return whitelist;
    }
    for (String whitelistedDataset : whitelistStr.split(",")) {
      whitelistedDataset = whitelistedDataset.trim();
      // whitelistedDataset should be of the form <project>:<dataset>, or just <dataset>
      // if it ends with ':', the admin provided an invalid entry in the whitelist and it should be ignored.
      if (whitelistedDataset.endsWith(":")) {
        continue;
      }
      int idx = whitelistedDataset.indexOf(':');
      if (idx > 0) {
        String datasetProject = whitelistedDataset.substring(0, idx);
        String datasetName = whitelistedDataset.substring(idx + 1);
        whitelist.add(DatasetId.of(datasetProject, datasetName));
      } else if (idx == 0) {
        // if the value is :<dataset>, treat it like it's just <dataset>
        whitelist.add(DatasetId.of(connectionProject, whitelistedDataset.substring(1)));
      } else {
        whitelist.add(DatasetId.of(connectionProject, whitelistedDataset));
      }
    }
    return whitelist;
  }

  private DatasetId getDatasetId(String datasetStr, String connectionProject) {
    int idx = datasetStr.indexOf(":");
    if (idx > 0) {
      String project = datasetStr.substring(0, idx);
      String name = datasetStr.substring(idx + 1);
      return DatasetId.of(project, name);
    }
    return DatasetId.of(connectionProject, datasetStr);
  }

  private Collection<Dataset> getDatasets(BigQuery bigQuery, Set<DatasetId> datasetWhitelist) {
    // this will include datasets that can be listed by the service account, but may not include all datasets
    // in the whitelist, if the whitelist contains publicly accessible datasets from other projects.
    // do some post-processing to filter out anything not in the whitelist and also try and lookup datasets
    // that are in the whitelist but not in the returned list
    Page<Dataset> datasets = bigQuery.listDatasets(BigQuery.DatasetListOption.all());
    Set<DatasetId> missingDatasets = new HashSet<>(datasetWhitelist);
    List<Dataset> output = new ArrayList<>();
    for (Dataset dataset : datasets.iterateAll()) {
      missingDatasets.remove(dataset.getDatasetId());
      if (datasetWhitelist.isEmpty() || datasetWhitelist.contains(dataset.getDatasetId())) {
        output.add(dataset);
      }
    }
    // this only contains datasets that are in the whitelist but were not returned by the list call
    for (DatasetId whitelistDataset : missingDatasets) {
      try {
        Dataset dataset = bigQuery.getDataset(whitelistDataset);
        if (dataset != null) {
          output.add(dataset);
        }
      } catch (BigQueryException e) {
        // ignore and move on
        LOG.debug("Exception getting dataset {} from the whitelist.", whitelistDataset, e);
      }
    }
    return output;
  }
}
