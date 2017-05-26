/*
 * Copyright © 2017 Cask Data, Inc.
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
package co.cask.wrangler.service.database;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.ArtifactRange;
import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

public class DBService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DBService.class);
  private static final Gson GSON = new Gson();
  private static final Map<String, String> MAP_CLASSNAME_TO_DRIVER_NAME =
    ImmutableMap.of("com.microsoft.sqlserver.jdbc.SQLServerDriver", "MSSQL",
                    "com.mysql.jdbc.Driver", "MySQL",
                    "org.netezza.Driver", "Netezza",
                    "org.postgresql.Driver", "PostGres");

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset table;

  /**
   * Return the list of tables and the number of columns in each of them.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @throws Exception
   */
  @Path("list")
  @POST
  public void list(HttpServiceRequest request, HttpServiceResponder responder) {
    DriverCleanup driverCleanup = null;
    try {
      List<ArtifactInfo> artifactInfos = getContext().listArtifacts();
      ArtifactInfo targetArtifactInfo = null;
      for (ArtifactInfo artifactInfo : artifactInfos) {
        Set<PluginClass> pluginClassSet = artifactInfo.getClasses().getPlugins();
        for (PluginClass plugin : pluginClassSet) {
          if (plugin.getType().equals("jdbc") && plugin.getName().equals("mysql")) {
            targetArtifactInfo = artifactInfo;
            break;
          }
        }
        if (targetArtifactInfo != null) {
          break;
        }
      }

      if (targetArtifactInfo == null) {
        responder.sendError(400, "Unable to find artifact with mysql plugin");
        return;
      }

      try (CloseableClassLoader closeableClassLoader =
        getContext().createClassLoader(targetArtifactInfo, null)) {
        Class<? extends Driver> driverClass = (Class<? extends Driver>)
          closeableClassLoader.loadClass("com.mysql.jdbc.Driver");
        String body = Bytes.toString(request.getContent());
        ListTablesRequest listTablesRequest = GSON.fromJson(body, ListTablesRequest.class);

        driverCleanup = ensureJDBCDriverIsAvailable(driverClass, listTablesRequest.connectionString, "jdbc", "mysql");
        Connection connection;
        if (listTablesRequest.userName == null) {
          connection = DriverManager.getConnection(listTablesRequest.connectionString);
        } else {
          connection = DriverManager.getConnection(listTablesRequest.connectionString, listTablesRequest.userName,
                                                   listTablesRequest.password);
        }
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, "%", null);
        List<TableInfo> tables = new ArrayList<>();
        while (resultSet.next()) {
          Statement statement = connection.createStatement();
          statement.setMaxRows(1);
          ResultSet queryResult =
            statement.executeQuery(String.format("select * from %s where 1=0", resultSet.getString(3)));
          tables.add(new TableInfo(resultSet.getString(3), queryResult.getMetaData().getColumnCount()));
        }
        responder.sendString(HttpURLConnection.HTTP_OK, new Gson().toJson(tables), Charsets.UTF_8);
      }
    } catch (Exception e) {
      LOG.error("Exception while getting tables", e);
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    } finally {
      if (driverCleanup != null) {
        driverCleanup.destroy();
      }
    }
  }

  class DriverInfo {
    String artifactName;
    String artifactVersion;
    ArtifactScope artifactScope;
    String pluginName;
    String pluginType;
    String pluginClassName;
    Set<ArtifactRange> parentArtifacts;

    DriverInfo(ArtifactInfo artifactInfo, PluginClass pluginClass) {
      this.artifactName = artifactInfo.getName();
      this.artifactVersion = artifactInfo.getVersion();
      this.artifactScope = artifactInfo.getScope();
      this.pluginName = pluginClass.getName();
      this.pluginType = pluginClass.getType();
      this.pluginClassName = pluginClass.getClassName();
      this.parentArtifacts = artifactInfo.getParents();
    }
  }

  /**
   * Return the list of tables and the number of columns in each of them.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @throws Exception
   */
  @Path("list/drivers")
  @GET
  public void driversList(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      Map<String, List<DriverInfo>> driverNameToInfoMap = new HashMap<>();

      List<ArtifactInfo> artifactInfos = getContext().listArtifacts();
      for (ArtifactInfo artifactInfo : artifactInfos) {
        Set<PluginClass> pluginClassSet = artifactInfo.getClasses().getPlugins();
        for (PluginClass plugin : pluginClassSet) {
          if (plugin.getType().equals("jdbc")) {
            if (MAP_CLASSNAME_TO_DRIVER_NAME.containsKey(plugin.getClassName())) {
              String driverName = MAP_CLASSNAME_TO_DRIVER_NAME.get(plugin.getClassName());
              if (!driverNameToInfoMap.containsKey(driverName)) {
                driverNameToInfoMap.put(driverName, new ArrayList<DriverInfo>());
              }
              driverNameToInfoMap.get(driverName).add(new DriverInfo(artifactInfo, plugin));
            } else {
              LOG.warn("Found JDBC driver of unrecognized classname {}", plugin.getClassName());
              continue;
            }
          }
        }
      }
      responder.sendJson(200, driverNameToInfoMap);
    } catch (IOException e) {
      responder.sendError(500, String.format("Error while listing drivers %s", e.getMessage()));
    }
  }


  class ListTablesRequest {
    String connectionString;
    @Nullable String userName;
    @Nullable String password;
    ListTablesRequest(String connectionString, @Nullable String userName, @Nullable String password) {
      this.connectionString = connectionString;
      this.userName = userName;
      this.password = password;
    }
  }

  class ExecuteQueryRequest extends ListTablesRequest {
    String query;
    ExecuteQueryRequest(String connectionString, String query,
                        @Nullable String userName, @Nullable String password) {
      super(connectionString, userName, password);
      this.query = query;
    }
  }

  /**
   * Return the list of tables and the number of columns in each of them.
   *
   * @param request HTTP Request Handler
   * @param responder HTTP Response Handler
   * @throws Exception
   */
  @Path("execute")
  @POST
  public void execute(HttpServiceRequest request, HttpServiceResponder responder) {
    String body = Bytes.toString(request.getContent());
    ExecuteQueryRequest queryRequest = GSON.fromJson(body, ExecuteQueryRequest.class);

    DriverCleanup driverCleanup = null;
    try {
      Class<? extends Driver> driverClass = getContext().loadPluginClass("mysql");
      driverCleanup = ensureJDBCDriverIsAvailable(driverClass, queryRequest.connectionString, "jdbc", "mysql");
      Connection connection;
      if (queryRequest.userName == null) {
        connection = DriverManager.getConnection(queryRequest.connectionString);
      } else {
        connection = DriverManager.getConnection(queryRequest.connectionString, queryRequest.userName,
                                                 queryRequest.password);
      }
      Statement statement = connection.createStatement();
      ResultSet resultSet =
        statement.executeQuery(queryRequest.query);

      String dbQuery = String.format("%s:%s", queryRequest.connectionString, queryRequest.query);
      String id = ServiceUtils.generateMD5(dbQuery);
      table.createWorkspaceMeta(id, queryRequest.query);

      List<Record> records = new ArrayList<>();
      int columns = resultSet.getMetaData().getColumnCount();
      List<Schema.Field> fields = getSchemaFields(resultSet);

      while (resultSet.next()) {
        Record record = new Record();
        for (int i = 1; i <= columns; i++) {
          record.add(fields.get(i-1).getName(), resultSet.getString(i));
        }
        records.add(record);
      }

      // Set all properties and write to workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.DB_CONFIG, GSON.toJson(queryRequest));
      table.writeProperties(id, properties);

      String data = GSON.toJson(records);
      table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, data.getBytes(Charsets.UTF_8));

      // just keeping this to be consistent with FilesystemExplorer response, not really needed to be in this format.
      JsonArray values = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty(PropertyIds.ID, id);
      values.add(object);

      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);

      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      LOG.error("Exception while getting tables", e);
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    } finally {
      if (driverCleanup != null) {
        driverCleanup.destroy();
      }
    }
  }


  public static List<Schema.Field> getSchemaFields(ResultSet resultSet) throws SQLException {
    List<Schema.Field> schemaFields = Lists.newArrayList();
    ResultSetMetaData metadata = resultSet.getMetaData();
    // ResultSetMetadata columns are numbered starting with 1
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      String columnName = metadata.getColumnName(i);
      int columnSqlType = metadata.getColumnType(i);
      Schema columnSchema = Schema.of(getType(columnSqlType));
      if (ResultSetMetaData.columnNullable == metadata.isNullable(i)) {
        columnSchema = Schema.nullableOf(columnSchema);
      }
      Schema.Field field = Schema.Field.of(columnName, columnSchema);
      schemaFields.add(field);
    }
    return schemaFields;
  }

  // given a sql type return schema type
  private static Schema.Type getType(int sqlType) throws SQLException {
    // Type.STRING covers sql types - VARCHAR,CHAR,CLOB,LONGNVARCHAR,LONGVARCHAR,NCHAR,NCLOB,NVARCHAR
    Schema.Type type = Schema.Type.STRING;
    switch (sqlType) {
      case Types.NULL:
        type = Schema.Type.NULL;
        break;

      case Types.BOOLEAN:
      case Types.BIT:
        type = Schema.Type.BOOLEAN;
        break;

      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        type = Schema.Type.INT;
        break;

      case Types.BIGINT:
        type = Schema.Type.LONG;
        break;

      case Types.REAL:
      case Types.FLOAT:
        type = Schema.Type.FLOAT;
        break;

      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.DOUBLE:
        type = Schema.Type.DOUBLE;
        break;

      case Types.DATE:
      case Types.TIME:
      case Types.TIMESTAMP:
        type = Schema.Type.LONG;
        break;

      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        type = Schema.Type.BYTES;
        break;

      case Types.ARRAY:
      case Types.DATALINK:
      case Types.DISTINCT:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.REF:
      case Types.ROWID:
      case Types.SQLXML:
      case Types.STRUCT:
        throw new SQLException(new UnsupportedTypeException("Unsupported SQL Type: " + sqlType));
    }

    return type;
  }

  class TableInfo {
    String tableName;
    int columnCount;

    TableInfo(String tableName, int columnCount) {
      this.tableName = tableName;
      this.columnCount = columnCount;
    }
  }

  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> jdbcDriverClass,
                                                          String connectionString,
                                                          String jdbcPluginType, String jdbcPluginName)
    throws IllegalAccessException, InstantiationException, SQLException {

    try {
      DriverManager.getDriver(connectionString);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      // Driver not found. We will try to register it with the DriverManager.
      LOG.debug("Plugin Type: {} and Plugin Name: {}; Driver Class: {} not found. Registering JDBC driver via shim {} ",
                jdbcPluginType, jdbcPluginName, jdbcDriverClass.getName(),
                JDBCDriverShim.class.getName());
      final JDBCDriverShim driverShim = new JDBCDriverShim(jdbcDriverClass.newInstance());
      try {
        deregisterAllDrivers(jdbcDriverClass);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.error("Unable to deregister JDBC Driver class {}", jdbcDriverClass);
      }
      LOG.info("Registering driver {} with Driver Manager", driverShim.getClass().getCanonicalName());
      DriverManager.registerDriver(driverShim);
      return new DriverCleanup(driverShim);
    }
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> driverClass)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DBService.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      LOG.debug("Removing non-null driver object from drivers list.");
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        LOG.debug("Found null classloader for default driver {}. Ignoring since this may be using system classloader.",
                  d.getClass().getName());
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(driverClass.getClassLoader())) {
        LOG.debug("Removing default driver {} from registeredDrivers", d.getClass().getName());
        list.remove(driverInfo);
      }
    }
  }

}
