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

package co.cask.wrangler.service.database;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
import co.cask.wrangler.proto.ConnectionSample;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.PluginSpec;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.connection.Connection;
import co.cask.wrangler.proto.connection.ConnectionMeta;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.proto.db.AllowedDriverInfo;
import co.cask.wrangler.proto.db.DBSpec;
import co.cask.wrangler.proto.db.JDBCDriverInfo;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.utils.ObjectSerDe;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Closeables;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
public class DatabaseHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseHandler.class);
  private static final String JDBC = "jdbc";

  // Driver class loaders cached.
  private final LoadingCache<String, CloseableClassLoader> cache = CacheBuilder.newBuilder()
    .expireAfterAccess(60, TimeUnit.MINUTES)
    .removalListener((RemovalListener<String, CloseableClassLoader>) removalNotification -> {
      CloseableClassLoader value = removalNotification.getValue();
      if (value != null) {
        try {
          Closeables.close(value, true);
        } catch (IOException e) {
          // never happens.
        }
      }
    }).build(new CacheLoader<String, CloseableClassLoader>() {
      @Override
      public CloseableClassLoader load(String name) throws Exception {
        List<ArtifactInfo> artifacts = getContext().listArtifacts();
        ArtifactInfo info = null;
        for (ArtifactInfo artifact : artifacts) {
          Set<PluginClass> pluginClassSet = artifact.getClasses().getPlugins();
          for (PluginClass plugin : pluginClassSet) {
            if (JDBC.equalsIgnoreCase(plugin.getType()) && plugin.getName().equals(name)) {
              info = artifact;
              break;
            }
          }
          if (info != null) {
            break;
          }
        }
        if (info == null) {
          throw new IllegalArgumentException(
            String.format("Database driver '%s' not found.", name)
          );
        }
        CloseableClassLoader closeableClassLoader = getContext().createClassLoader(info, null);
        return closeableClassLoader;
      }
    });

  private final class DriverInfo {
    private String jdbcUrlPattern;
    private String name;
    private String tag;
    private String port;

    DriverInfo(String name, String jdbcUrlPattern, String tag, String port) {
      this.name = name;
      this.jdbcUrlPattern = jdbcUrlPattern;
      this.tag = tag;
      this.port = port;
    }

    public String getJdbcUrlPattern() {
      return jdbcUrlPattern;
    }

    public String getName() {
      return name;
    }

    public String getTag() {
      return tag;
    }

    public String getPort() {
      return port;
    }
  }

  /**
   * Executes something using a SQL connection.
   */
  public interface Executor {
    void execute(java.sql.Connection connection) throws Exception;
  }

  private final Multimap<String, DriverInfo> drivers = ArrayListMultimap.create();

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    drivers.clear();
    InputStream is = DatabaseHandler.class.getClassLoader().getResourceAsStream("drivers.mapping");
    if (is == null) {
      // shouldn't happen unless packaging changes
      LOG.error("Unable to get JDBC driver mapping.");
      return;
    }
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] columns = line.split(",");
        if (columns.length == 5) {
          DriverInfo info = new DriverInfo(columns[0], columns[2], columns[3], columns[4]);
          drivers.put(columns[1].trim(), info);
        }
      }
    }
  }

  @GET
  @Path("jdbc/drivers")
  public void listDrivers(HttpServiceRequest request, HttpServiceResponder responder) {
    listDrivers(request, responder, getContext().getNamespace());
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
  public void listDrivers(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      List<JDBCDriverInfo> values = new ArrayList<>();
      List<ArtifactInfo> artifacts = getContext().listArtifacts();
      for (ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        for (PluginClass plugin : plugins) {
          String type = plugin.getType();
          if (!JDBC.equalsIgnoreCase(type)) {
            continue;
          }
          String className = plugin.getClassName();
          if (!drivers.containsKey(className)) {
            continue;
          }
          Collection<DriverInfo> infos = drivers.get(className);
          for (DriverInfo info : infos) {
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
      }
      return new ServiceResponse<>(values);
    });
  }

  @GET
  @Path("jdbc/allowed")
  public void listAvailableDrivers(HttpServiceRequest request, HttpServiceResponder responder) {
    listAvailableDrivers(request, responder, getContext().getNamespace());
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
  public void listAvailableDrivers(HttpServiceRequest request, HttpServiceResponder responder,
                                   @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      List<AllowedDriverInfo> values = new ArrayList<>();
      Collection<Map.Entry<String, DriverInfo>> entries = drivers.entries();
      for (Map.Entry<String, DriverInfo> driver : entries) {
        String shortTag = driver.getValue().getTag();
        values.add(new AllowedDriverInfo(driver.getKey(), driver.getValue().getName(), shortTag, shortTag,
                                         driver.getValue().getPort()));
      }
      return new ServiceResponse<>(values);
    });
  }

  /**
   * Tests the connection.
   *
   * Following is the response when the connection is successfull.
   *
   * {
   *   "status" : 200,
   *   "message" : "Successfully connected to database."
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("connections/jdbc/test")
  public void testConnection(HttpServiceRequest request, HttpServiceResponder responder) {
    respond(request, responder, () -> {
      DriverCleanup cleanup = null;
      try {
        // Extract the body of the request and transform it to the Connection object.
        RequestExtractor extractor = new RequestExtractor(request);
        ConnectionMeta connection = extractor.getConnectionMeta(ConnectionType.DATABASE);

        cleanup = loadAndExecute(connection, java.sql.Connection::getMetaData);
        return new ServiceResponse<>("Successfully connected to database.");
      } finally {
        if (cleanup != null) {
          cleanup.destroy();
        }
      }
    });
  }

  @POST
  @Path("connections/databases")
  public void listDatabases(HttpServiceRequest request, HttpServiceResponder responder) {
    listDatabases(request, responder, getContext().getNamespace());
  }

  /**
   * Lists all databases.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("contexts/{context}/connections/databases")
  public void listDatabases(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      DriverCleanup cleanup = null;
      try {
        // Extract the body of the request and transform it to the Connection object.
        RequestExtractor extractor = new RequestExtractor(request);
        ConnectionMeta conn = extractor.getConnectionMeta(ConnectionType.DATABASE);

        List<String> values = new ArrayList<>();
        cleanup = loadAndExecute(conn, connection -> {
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
        });
        return new ServiceResponse<>(values);
      } finally {
        if (cleanup != null) {
          cleanup.destroy();
        }
      }
    });
  }

  @GET
  @Path("connections/{id}/tables")
  public void listTables(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("id") String id) {
    listTables(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Lists all the tables within a database.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/tables")
  public void listTables(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      DriverCleanup cleanup = null;
      try {
        List<Name> values = new ArrayList<>();
        cleanup = loadAndExecute(store.get(new NamespacedId(namespace, id)), connection -> {
          String product = connection.getMetaData().getDatabaseProductName().toLowerCase();
          ResultSet resultSet;
          if (product.equalsIgnoreCase("oracle")) {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT table_name FROM all_tables");
          } else {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getTables(null, null, "%", null);
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
        });
        return new ServiceResponse<>(values);
      } finally {
        if (cleanup != null) {
          cleanup.destroy();
        }
      }
    });
  }

  @GET
  @Path("connections/{id}/tables/{table}/read")
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("id") String id, @PathParam("table") String table,
                   @QueryParam("lines") int lines,
                   @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    read(request, responder, getContext().getNamespace(), id, table, lines, scope);
  }

  /**
   * Reads a table into workspace.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   * @param table Name of the database table.
   * @param lines No of lines to be read from RDBMS table.
   * @param scope Group the workspace should be created in.
   */
  @GET
  @Path("contexts/{context}/connections/{id}/tables/{table}/read")
  public void read(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace, @PathParam("id") String id, @PathParam("table") String table,
                   @QueryParam("lines") int lines,
                   @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, () -> {
      DriverCleanup cleanup = null;
      try {
        AtomicReference<ConnectionSample> sampleRef = new AtomicReference<>();
        cleanup = loadAndExecute(store.get(new NamespacedId(namespace, id)), connection -> {
          try (Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(String.format("select * from %s", table))) {
            List<Row> rows = getRows(lines, result);

            String identifier = ServiceUtils.generateMD5(table);
            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyIds.ID, identifier);
            properties.put(PropertyIds.NAME, table);
            properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.DATABASE.getType());
            properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
            properties.put(PropertyIds.CONNECTION_ID, id);
            NamespacedId namespacedId = new NamespacedId(namespace, identifier);
            WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(namespacedId, table)
              .setScope(scope)
              .setProperties(properties)
              .build();
            ws.writeWorkspaceMeta(workspaceMeta);

            ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
            byte[] data = serDe.toByteArray(rows);
            ws.updateWorkspaceData(namespacedId, DataType.RECORDS, data);

            ConnectionSample sample = new ConnectionSample(namespacedId.getId(), table,
                                                           ConnectionType.DATABASE.getType(),
                                                           SamplingMethod.NONE.getMethod(), id);
            sampleRef.set(sample);
          }
        });
        return new ServiceResponse<>(sampleRef.get());
      } finally {
        if (cleanup != null) {
          cleanup.destroy();
        }
      }
    });
  }

  @VisibleForTesting
  public static List<Row> getRows(int lines, ResultSet result) throws SQLException {
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

  @Path("connections/{id}/tables/{table}/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("id") String id, @PathParam("table") String table) {
    specification(request, responder, getContext().getNamespace(), id, table);
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the connection.
   * @param table in the database.
   */
  @Path("contexts/{context}/connections/{id}/tables/{table}/specification")
  @GET
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace, @PathParam("id") String id,
                            @PathParam("table") String table) {
    respond(request, responder, namespace, () -> {
      Connection conn = store.get(new NamespacedId(namespace, id));

      Map<String, String> properties = new HashMap<>();
      properties.put("connectionString", conn.getProperties().get("url"));
      properties.put("referenceName", table);
      properties.put("user", conn.getProperties().get("username"));
      properties.put("password", conn.getProperties().get("password"));
      properties.put("importQuery", String.format("SELECT * FROM %s WHERE $CONDITIONS", table));
      properties.put("numSplits", "1");
      properties.put("jdbcPluginName", conn.getProperties().get("name"));
      properties.put("jdbcPluginType", conn.getProperties().get("type"));

      PluginSpec pluginSpec = new PluginSpec(String.format("Database - %s", table), "source", properties);
      DBSpec spec = new DBSpec(pluginSpec);

      return new ServiceResponse<>(spec);
    });
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
   * @return pair of connection and the driver cleanup.
   */
  private DriverCleanup loadAndExecute(ConnectionMeta connection, Executor executor) throws Exception {
    DriverCleanup cleanup = null;
    String name = connection.getProperties().get("name");
    String classz = connection.getProperties().get("class");
    String url = connection.getProperties().get("url");
    String username = connection.getProperties().get("username");
    String password = connection.getProperties().get("password");

    CloseableClassLoader closeableClassLoader = cache.get(name);
    Class<? extends Driver> driverClass = (Class<? extends Driver>) closeableClassLoader.loadClass(classz);
    cleanup = ensureJDBCDriverIsAvailable(driverClass, url);
    java.sql.Connection conn = DriverManager.getConnection(url, username, password);
    executor.execute(conn);
    return cleanup;
  }

  public static DriverCleanup ensureJDBCDriverIsAvailable(Class<? extends Driver> classz, String url)
    throws IllegalAccessException, InstantiationException, SQLException {
    try {
      DriverManager.getDriver(url);
      return new DriverCleanup(null);
    } catch (SQLException e) {
      Driver driver = classz.newInstance();
      final JDBCDriverShim shim = new JDBCDriverShim(driver);
      try {
        deregisterAllDrivers(classz);
        DriverManager.registerDriver(shim);
        return new DriverCleanup(shim);
      } catch (NoSuchFieldException | ClassNotFoundException e1) {
        LOG.warn("Unable to deregister JDBC Driver class {}", classz);
      }
      return null;
    }
  }

  /**
   * De-register all SQL drivers that are associated with the class
   */
  public static void deregisterAllDrivers(Class<? extends Driver> classz)
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
  public static class Name {
    private final String name;
    private final int count;

    public Name(String name) {
      this.name = name;
      // TODO: check whether this can be removed
      this.count = 0;
    }
  }
}
