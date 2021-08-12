/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.database;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginConfigurer;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
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
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.PluginSpec;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionMeta;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.db.AllowedDriverInfo;
import io.cdap.wrangler.proto.db.DBSpec;
import io.cdap.wrangler.proto.db.JDBCDriverInfo;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.macro.ServiceMacroEvaluator;
import io.cdap.wrangler.utils.ObjectSerDe;
import io.cdap.wrangler.utils.ReferenceNames;
import org.apache.commons.lang3.text.StrLookup;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;


/**
 * Class description here.
 */
@Deprecated
public class DatabaseHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseHandler.class);
  private static final List<String> MACRO_FIELDS = ImmutableList.of("username", "password");
  private static final String JDBC = "jdbc";
  private final Map<String, ServiceMacroEvaluator> macroEvaluators = new HashMap<>();

  static final class DriverInfo {
    private final String jdbcUrlPattern;
    private final String className;
    private final String name;
    private final String tag;
    private final String port;
    private final boolean basicAllowed;

    DriverInfo(String name, String className, String jdbcUrlPattern, String tag, String port, boolean basicAllowed) {
      this.name = name;
      this.className = className;
      this.jdbcUrlPattern = jdbcUrlPattern;
      this.tag = tag;
      this.port = port;
      this.basicAllowed = basicAllowed;
    }

    String getJdbcUrlPattern() {
      return jdbcUrlPattern;
    }

    String getName() {
      return name;
    }

    String getTag() {
      return tag;
    }

    String getPort() {
      return port;
    }

  }

  /**
   * Executes something using a SQL connection.
   */
  public interface Executor {
    void execute(java.sql.Connection connection) throws Exception;
  }

  private final Map<String, DriverInfo> drivers = new HashMap<>();

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    drivers.clear();
    InputStream is = DatabaseHandler.class.getClassLoader().getResourceAsStream("drivers.mapping");
    if (is == null) {
      // shouldn't happen unless packaging changes
      LOG.error("Unable to get JDBC driver mapping.");
      return;
    }
    loadDrivers(is, drivers);
  }

  @VisibleForTesting
  static void loadDrivers(InputStream is, Map<String, DriverInfo> drivers) throws IOException {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] columns = line.split(",");
        if (columns.length == 6) {
          DriverInfo info = new DriverInfo(columns[0], columns[1], columns[2], columns[3], columns[4],
                                           Boolean.parseBoolean(columns[5]));
          if (drivers.containsKey(info.tag)) {
            // this only happens if the drivers.mapping file is invalid
            // TODO: (CDAP-15353) wrangler should not assume names are unique
            throw new IllegalStateException(
              "Wrangler's allowed JDBC plugins has been misconfigured. "
                + "Please make sure the driver plugin names in the drivers.mapping file are unique.");
          }
          drivers.put(info.tag, info);
        }
      }
    }
  }

  /**
   * Lists all the JDBC drivers installed.
   * <p>
   * Following is JSON Response
   * {
   *  "count": 1,
   *  "message": "Success",
   *  "status": 200,
   *  "values": [
   *    {
   *      "label": "MySQL",
   *      "version": "5.1.39"
   *      "url": "jdbc:mysql://${hostname}:${port}/${database}?user=${username}&password=${password}"
   *      "default.port" : "3306",
   *      "properties": {
   *        "class": "com.mysql.jdbc.Driver",
   *        "name": "mysql",
   *        "type": "jdbc",
   *      },
   *      "required" : [
   *        "hostname",
   *        "port",
   *        "database",
   *        "username",
   *        "password",
   *        "url"
   *      ]
   *    }
   *   ]
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("contexts/{context}/jdbc/drivers")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listDrivers(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      List<JDBCDriverInfo> values = new ArrayList<>();
      List<ArtifactInfo> artifacts = getContext().listArtifacts(namespace);
      for (ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        for (PluginClass plugin : plugins) {
          String type = plugin.getType();
          if (!JDBC.equalsIgnoreCase(type)) {
            continue;
          }
          // TODO: (CDAP-15353) Wrangler should not assume plugin names are unique
          // if info is null, it means it's not an 'allowed' driver from the drivers.mapping file that is returned by
          // the jdbc/allowed endpoint.
          if (!drivers.containsKey(plugin.getName())) {
            continue;
          }
          DriverInfo info = drivers.get(plugin.getName());
          Map<String, String> properties = new HashMap<>();
          properties.put("class", plugin.getClassName());
          properties.put("type", plugin.getType());
          properties.put("name", plugin.getName());
          List<String> fields = getMacros(info.getJdbcUrlPattern());
          fields.add("url");

          JDBCDriverInfo jdbcDriverInfo =
            new JDBCDriverInfo(info.getName(), artifact.getVersion(), info.getJdbcUrlPattern(), info.getPort(),
                               fields, properties);
          values.add(jdbcDriverInfo);
        }
      }
      return new ServiceResponse<>(values);
    });
  }

  /**
   * List all the possible drivers supported.
   *
   * Following is the sample response
   *
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 1,
   *   "values" : [
   *      {
   *        "class" : "com.jdbc.sql.Driver",
   *        "label" : "mysql",
   *        "name" : "mysql"
   *      }
   *   ]
   * }
   *
   * TODO: figure out why this and the listDrivers() method both exist and why they have different response fields
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("contexts/{context}/jdbc/allowed")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listAvailableDrivers(HttpServiceRequest request, HttpServiceResponder responder,
                                   @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      List<AllowedDriverInfo> values = new ArrayList<>();
      for (DriverInfo driver : drivers.values()) {
        values.add(new AllowedDriverInfo(driver.className, driver.name, driver.tag, driver.tag,
                                         driver.port, driver.basicAllowed));
      }
      return new ServiceResponse<>(values);
    });
  }

  @POST
  @Path("contexts/{context}/connections/jdbc/test")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void testConnection(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.DATABASE);

      loadAndExecute(ns, connection, java.sql.Connection::getMetaData, getContext());
      return new ServiceResponse<>("Successfully connected to database.");
    });
  }

  /**
   * Lists all databases.
   *
   * @param request HTTP requests handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("contexts/{context}/connections/databases")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listDatabases(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      ConnectionMeta conn = extractor.getConnectionMeta(ConnectionType.DATABASE);

      List<String> values = new ArrayList<>();
      loadAndExecute(ns, conn, connection -> {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet resultSet;
        PreparedStatement ps = null;
        if ("postgresql".equals(connection.getMetaData().getDatabaseProductName().toLowerCase().trim())) {
          ps = connection.prepareStatement(
            "SELECT datname AS TABLE_CAT FROM pg_database WHERE datistemplate = false;"
          );
          resultSet = ps.executeQuery();
        } else {
          resultSet = metaData.getCatalogs();
        }
        try {
          while (resultSet.next()) {
            values.add(resultSet.getString("TABLE_CAT"));
          }
        } finally {
          resultSet.close();
          if (ps != null) {
            ps.close();
          }
        }
      }, getContext());
      return new ServiceResponse<>(values);
    });
  }

  /**
   * Lists all the tables within a database.
   *
   * @param request HTTP requests handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/tables")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void listTables(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      List<Name> values = new ArrayList<>();
      Connection conn = getConnection(new NamespacedId(ns, id));
      loadAndExecute(ns, conn, connection -> {
        String product = connection.getMetaData().getDatabaseProductName().toLowerCase();
        ResultSet resultSet;
        if (product.equalsIgnoreCase("oracle")) {
          Statement statement = connection.createStatement();
          resultSet = statement.executeQuery("SELECT table_name FROM all_tables");
        } else {
          DatabaseMetaData metaData = connection.getMetaData();
          resultSet = metaData.getTables(connection.getCatalog(), connection.getSchema(), "%", null);
        }
        try {
          while (resultSet.next()) {
            String name;
            if (product.equalsIgnoreCase("oracle")) {
              name = resultSet.getString("table_name");
              if (name.contains("$")) {
                continue;
              }
            } else {
              name = resultSet.getString(3);
            }
            values.add(new Name(name));
          }
        } finally {
          if (resultSet != null) {
            resultSet.close();
          }
        }
      }, getContext());
      return new ServiceResponse<>(values);
    });
  }

  /**
   * Reads a table into workspace.
   *
   * @param request HTTP requests handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   * @param table Name of the database table.
   * @param lines No of lines to be read from RDBMS table.
   * @param scope Group the workspace should be created in.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/tables/{table}/read")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace, @PathParam("id") String id, @PathParam("table") String table,
                   @QueryParam("lines") int lines,
                   @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> {
      AtomicReference<ConnectionSample> sampleRef = new AtomicReference<>();
      Connection conn = getConnection(new NamespacedId(ns, id));

      loadAndExecute(ns, conn, connection -> {
        try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(String.format("select * from %s", table))) {
          List<Row> rows = getRows(lines, result);

          Map<String, String> properties = new HashMap<>();
          properties.put(PropertyIds.NAME, table);
          properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.DATABASE.getType());
          properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
          properties.put(PropertyIds.CONNECTION_ID, id);
          WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(table)
            .setScope(scope)
            .setProperties(properties)
            .build();
          String sampleId = TransactionRunners.run(getContext(), context -> {
            WorkspaceDataset ws = WorkspaceDataset.get(context);
            NamespacedId workspaceId = ws.createWorkspace(ns, workspaceMeta);

            ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
            byte[] data = serDe.toByteArray(rows);
            ws.updateWorkspaceData(workspaceId, DataType.RECORDS, data);
            return workspaceId.getId();
          });

          ConnectionSample sample = new ConnectionSample(sampleId, table,
                                                         ConnectionType.DATABASE.getType(),
                                                         SamplingMethod.NONE.getMethod(), id);
          sampleRef.set(sample);
        }
      }, getContext());
      return new ServiceResponse<>(sampleRef.get());
    });
  }

  @VisibleForTesting
  static List<Row> getRows(int lines, ResultSet result) throws SQLException {
    List<Row> rows = new ArrayList<>();
    ResultSetMetaData meta = result.getMetaData();
    int count = lines;
    while (result.next() && count > 0) {
      Row row = new Row();
      for (int i = 1; i < meta.getColumnCount() + 1; ++i) {
        Object object = result.getObject(i);
        if (object != null) {
          if (object instanceof Date) {
            object = ((Date) object).toLocalDate();
          } else if (object instanceof Time) {
            object = ((Time) object).toLocalTime();
          } else if (object instanceof Timestamp) {
            object = ((Timestamp) object).toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
          } else if (object.getClass().getName().equals("oracle.sql.ROWID")) {
            // If the object is Oracle ROWID, then convert it into a string.
            object = object.toString();
          }
        }
        row.add(meta.getColumnName(i), object);
      }
      rows.add(row);
      count--;
    }
    return rows;
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the connection.
   * @param table in the database.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/tables/{table}/specification")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("id") String id,
                            @PathParam("table") String table) {
    respond(request, responder, namespace, ns -> {
      Connection conn = getConnection(new NamespacedId(ns, id));

      PluginSpec pluginSpec = new PluginSpec(String.format("Database - %s", table), "source",
                                             getSpecification(conn, table));
      DBSpec spec = new DBSpec(pluginSpec);

      return new ServiceResponse<>(spec);
    });
  }

  public static Map<String, String> getSpecification(Connection conn, String table) {
    Map<String, String> properties = new HashMap<>();
    properties.put("connectionString", conn.getProperties().get("url"));
    properties.put("referenceName", ReferenceNames.cleanseReferenceName(table));
    properties.put("user", conn.getProperties().get("username"));
    properties.put("password", conn.getProperties().get("password"));
    properties.put("importQuery", String.format("SELECT * FROM %s WHERE $CONDITIONS", table));
    properties.put("numSplits", "1");
    properties.put("jdbcPluginName", conn.getProperties().get("name"));
    properties.put("jdbcPluginType", conn.getProperties().get("type"));
    return properties;
  }

  public static Map<String, String> getConnectorProperties(Map<String, String> config) {
    Map<String, String> properties = new HashMap<>();
    properties.put("connectionString", config.get("url"));
    properties.put("user", config.get("username"));
    properties.put("password", config.get("password"));
    properties.put("jdbcPluginType", config.get("type"));
    properties.put("jdbcPluginName", config.get("name"));
    return properties;
  }

  // TODO: this path will not work with current db connector if the database type supports schema,
  //  but it should still give back the related source information. Passing a root path so it will not interact with
  //  the actual database due to messed schema/table name
  public static String getPath(Workspace workspace) {
    return "/";
  }

  /**
   * Extracts all the macros within a string expression.
   *
   * @param expression specifies the expression.
   * @return list of macros present within an expression.
   */
  private List<String> getMacros(String expression) {
    final List<String> variables = new ArrayList<>();
    StrSubstitutor substitutor = new StrSubstitutor(new StrLookup() {
      @Override
      public String lookup(String s) {
        variables.add(s);
        return s;
      }
    });
    substitutor.replace(expression);
    return variables;
  }

  /**
   * Loads the driver and gets the connection to the database.
   *
   * @param connection the connection to be connected to.
   */
  private void loadAndExecute(Namespace namespace, ConnectionMeta connection,
                              Executor executor, SystemHttpServiceContext context) throws Exception {
    String jdbcPluginName = connection.getProperties().get("name");
    String url = connection.getProperties().get("url");

    PluginConfigurer pluginConfigurer = context.createPluginConfigurer(namespace.getName());
    Class<? extends Driver> driverClass = pluginConfigurer.usePluginClass(JDBC, jdbcPluginName,
                                                                          UUID.randomUUID().toString(),
                                                                          PluginProperties.builder().build());
    if (driverClass == null) {
      throw new NotFoundException("Could not find a JDBC driver plugin named " + jdbcPluginName);
    }

    DriverCleanup cleanup = ensureJDBCDriverIsAvailable(driverClass, url);
    try {
      Map<String, String> evaluated = evaluateMacros(connection, context, namespace.getName());
      String username = evaluated.get("username");
      String password = evaluated.get("password");

      java.sql.Connection conn = DriverManager.getConnection(url, username, password);
      executor.execute(conn);
    } finally {
      cleanup.destroy();
    }
  }

  /**
   * Evaluates all the MACRO_FIELDS.
   */
  private Map<String, String> evaluateMacros(ConnectionMeta connection, SystemHttpServiceContext context,
                                             String namespaceName) {
    Map<String, String> toEvaluate = new HashMap<>();
    for (String field : MACRO_FIELDS) {
      toEvaluate.put(field, connection.getProperties().get(field));
    }

    macroEvaluators.putIfAbsent(namespaceName, new ServiceMacroEvaluator(namespaceName, context));
    return context.evaluateMacros(namespaceName, toEvaluate, macroEvaluators.get(namespaceName));
  }

  private static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> classz, String url)
    throws IllegalAccessException, InstantiationException, SQLException {
    try {
      DriverManager.getDriver(url);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      Driver driver = classz.newInstance();
      final JDBCDriverShim shim = new JDBCDriverShim(driver);
      try {
        deregisterAllDrivers(classz);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.warn("Unable to deregister JDBC Driver class {}", classz);
      }
      DriverManager.registerDriver(shim);
      return new DriverCleanup(shim);
    }
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  private static void deregisterAllDrivers(Class<? extends Driver> classz)
    throws NoSuchFieldException, IllegalAccessException, ClassNotFoundException {
    Field field = DriverManager.class.getDeclaredField("registeredDrivers");
    field.setAccessible(true);
    List<?> list = (List<?>) field.get(null);
    for (Object driverInfo : list) {
      Class<?> driverInfoClass = DatabaseHandler.class.getClassLoader().loadClass("java.sql.DriverInfo");
      Field driverField = driverInfoClass.getDeclaredField("driver");
      driverField.setAccessible(true);
      Driver d = (Driver) driverField.get(driverInfo);
      if (d == null) {
        LOG.debug("Found null driver object in drivers list. Ignoring.");
        continue;
      }
      ClassLoader registeredDriverClassLoader = d.getClass().getClassLoader();
      if (registeredDriverClassLoader == null) {
        continue;
      }
      // Remove all objects in this list that were created using the classloader of the caller.
      if (d.getClass().getClassLoader().equals(classz.getClassLoader())) {
        list.remove(driverInfo);
      }
    }
  }

  /**
   * Table name object.
   */
  @SuppressWarnings({"FieldCanBeLocal", "unused"})
  private static class Name {
    private final String name;
    private final int count;

    Name(String name) {
      this.name = name;
      // TODO: check whether this can be removed
      this.count = 0;
    }
  }
}
