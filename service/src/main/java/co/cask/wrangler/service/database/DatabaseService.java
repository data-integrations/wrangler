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
import co.cask.cdap.api.artifact.CloseableClassLoader;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.dataset.connections.Connection;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.connections.ConnectionType;
import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.text.StrLookup;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.service.directive.DirectivesService.WORKSPACE_DATASET;

/**
 * Class description here.
 */
public class DatabaseService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatabaseService.class);
  private static final String JDBC = "jdbc";

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset ws;

  // Data Prep store which stores all the information associated with dataprep.
  @UseDataSet(DataPrep.DATAPREP_DATASET)
  private Table table;

  // Abstraction over the table defined above for managing connections.
  private ConnectionStore store;

  private static final Gson gson =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  // Driver class loaders cached.
  private final LoadingCache<String, CloseableClassLoader> cache = CacheBuilder.newBuilder()
    .expireAfterAccess(60, TimeUnit.MINUTES)
    .removalListener(new RemovalListener<String, CloseableClassLoader>() {
      @Override
      public void onRemoval(RemovalNotification<String, CloseableClassLoader> removalNotification) {
        CloseableClassLoader value = removalNotification.getValue();
        if (value != null) {
          try {
            Closeables.close(value, true);
          } catch (IOException e) {
            // never happens.
          }
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

    public DriverInfo(String name, String jdbcUrlPattern, String tag) {
      this.name = name;
      this.jdbcUrlPattern = jdbcUrlPattern;
      this.tag = tag;
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
  }

  public interface Executor {
    public void execute(java.sql.Connection connection) throws Exception;
  }

  private static final Map<String, DriverInfo> drivers = new HashMap<>();

  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new ConnectionStore(table);
    InputStream is = DatabaseService.class.getClassLoader().getResourceAsStream("drivers.mapping");
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line;
      while((line = br.readLine()) != null) {
        String[] columns = line.split(",");
        if (columns.length == 4) {
          DriverInfo info = new DriverInfo(columns[0], columns[2], columns[3]);
          drivers.put(columns[1].trim(), info);
        }
      }
      br.close();
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }

  /**
   * Lists all the JDBC drivers installed.
   *
   * Following is JSON Response
   * {
   *    "count": 1,
   *    "message": "Success",
   *    "status": 200,
   *    "values": [
   *      {
   *        "label": "MySQL",
   *        "version": "5.1.39"
   *        "url": "jdbc:mysql://${hostname}:${port}/${database}?user=${username}&password=${password}"
   *        "properties": {
   *          "class": "com.mysql.jdbc.Driver",
   *          "name": "mysql",
   *          "type": "jdbc",
   *        },
   *        "required" : [
   *          "hostname",
   *          "port",
   *          "database",
   *          "username",
   *          "password",
   *          "url"
   *        ]
   *      }
   *    ]
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("jdbc/drivers")
  public void listDrivers(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();
      List<ArtifactInfo> artifacts = getContext().listArtifacts();
      for (ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        for (PluginClass plugin : plugins) {
          String type = plugin.getType();
          if(JDBC.equalsIgnoreCase(type)) {
            JsonObject object = new JsonObject();
            String className = plugin.getClassName();
            if (drivers.containsKey(className)) {
              DriverInfo info = drivers.get(className);
              object.addProperty("label", info.getName());
              object.addProperty("version", artifact.getVersion());
              object.addProperty("url", info.getJdbcUrlPattern());
              JsonObject properties = new JsonObject();
              properties.addProperty("class", plugin.getClassName());
              properties.addProperty("type", plugin.getType());
              properties.addProperty("name", plugin.getName());
              JsonArray required = new JsonArray();
              List<String> fields = getMacros(info.getJdbcUrlPattern());
              fields.add("url");
              for (String field : fields) {
                required.add(new JsonPrimitive(field));
              }
              object.add("properties", properties);
              object.add("fields", required);
              values.add(object);
            }
          }
        }
      }
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("values", values);
      ServiceUtils.sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
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
   * @param request  HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("jdbc/allowed")
  public void listAvailableDrivers(HttpServiceRequest request, HttpServiceResponder responder) {
    JsonObject response = new JsonObject();
    JsonArray values = new JsonArray();
    for(Map.Entry<String, DriverInfo> driver : drivers.entrySet()) {
      JsonObject object = new JsonObject();
      object.addProperty("class", driver.getKey());
      object.addProperty("label", driver.getValue().getName());
      object.addProperty("tag", driver.getValue().getTag());
      String name = driver.getValue().getName();
      name = name.trim();
      name = name.toLowerCase();
      name = name.replaceAll("[^a-zA-Z0-9_]", "");
      object.addProperty("name",name);
      values.add(object);
    }
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", values.size());
    response.add("values", values);
    ServiceUtils.sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
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
   * @param request  HTTP request handler.
   * @param responder HTTP response handler.
   */
  @POST
  @Path("connections/jdbc/test")
  public void testConnection(HttpServiceRequest request, final HttpServiceResponder responder) {
    DriverCleanup cleanup = null;
    try {
      // Extract the body of the request and transform it to the Connection object.
      RequestExtractor extractor = new RequestExtractor(request);
      Connection connection = extractor.getContent("utf-8", Connection.class);

      if (ConnectionType.fromString(connection.getType().getType()) == ConnectionType.UNDEFINED) {
        error(responder, "Invalid connection type set.");
        return;
      }

      cleanup = loadAndExecute(connection, new Executor() {
        @Override
        public void execute(java.sql.Connection connection) throws Exception {
          connection.getMetaData();
          JsonObject response = new JsonObject();
          response.addProperty("status", HttpURLConnection.HTTP_OK);
          response.addProperty("message", "Successfully connected to database.");
          sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
        }
      });
    } catch (Exception e) {
      error(responder, e.getMessage());
    } finally {
      if (cleanup != null) {
        cleanup.destroy();
      }
    }
  }

  /**
   * Lists all databases.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which all the databses should be listed.
   */
  @GET
  @Path("connections/{id}/databases")
  public void listDatabases(HttpServiceRequest request, final HttpServiceResponder responder,
                         @PathParam("id") String id) {
    final JsonObject response = new JsonObject();
    final JsonArray values = new JsonArray();
    DriverCleanup cleanup = null;
    try {
      cleanup = loadAndExecute(id, new Executor() {
        @Override
        public void execute(java.sql.Connection connection) throws Exception {
          DatabaseMetaData metaData = connection.getMetaData();
          try(ResultSet resultSet = metaData.getCatalogs()) {
            while (resultSet.next()) {
              values.add(new JsonPrimitive(resultSet.getString("TABLE_CAT")));
            }
            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success.");
            response.addProperty("count", values.size());
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
          }
        }
      });
    } catch (Exception e) {
      error(responder, e.getMessage());
    } finally {
      if (cleanup != null) {
        cleanup.destroy();
      }
    }
  }

  /**
   * Lists all the tables within a database.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("connections/{id}/tables")
  public void listTables(HttpServiceRequest request, final HttpServiceResponder responder,
                         @PathParam("id") String id) {
    final JsonObject response = new JsonObject();
    final JsonArray values = new JsonArray();
    DriverCleanup cleanup = null;
    try {
      cleanup = loadAndExecute(id, new Executor() {
        @Override
        public void execute(java.sql.Connection connection) throws Exception {
          String product = connection.getMetaData().getDatabaseProductName().toLowerCase();
          ResultSet resultSet;
          if(product.equalsIgnoreCase("oracle")) {
            Statement statement = connection.createStatement();
            resultSet = statement.executeQuery("SELECT table_name FROM users_tables");
          } else {
            DatabaseMetaData metaData = connection.getMetaData();
            resultSet = metaData.getTables(null, null, "%", null);
          }
          try {
            while (resultSet.next()) {
              String name;
              if(product.equalsIgnoreCase("oracle")) {
                name = resultSet.getString("table_name");
                if(name.contains("$")) {
                  continue;
                }
              } else {
                name = resultSet.getString(3);
              }
              Statement statement = connection.createStatement();
              statement.setMaxRows(1);
              ResultSet queryResult =
                statement.executeQuery(
                  String.format("select * from %s where 1=0", name)
                );
              JsonObject object = new JsonObject();
              object.addProperty("name", name);
              object.addProperty("count", queryResult.getMetaData().getColumnCount());
              values.add(object);
            }
            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success");
            response.addProperty("count", values.size());
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
          } finally {
            if(resultSet != null) {
              resultSet.close();
            }
          }
        }
      });
    } catch (Exception e) {
      error(responder, e.getMessage());
    } finally {
      if (cleanup != null) {
        cleanup.destroy();
      }
    }
  }

  /**
   * Lists all the tables within a database.
   *
   * @param request HTTP requets handler.
   * @param responder HTTP response handler.
   * @param id Connection id for which the tables need to be listed from database.
   */
  @GET
  @Path("connections/{id}/tables/{table}/read")
  public void read(HttpServiceRequest request, final HttpServiceResponder responder,
                   @PathParam("id") String id, @PathParam("table") final String table,
                   @QueryParam("lines") final int lines) {
    final JsonObject response = new JsonObject();
    DriverCleanup cleanup = null;
    try {
      cleanup = loadAndExecute(id, new Executor() {
        @Override
        public void execute(java.sql.Connection connection) throws Exception {
          try (Statement statement = connection.createStatement();
               ResultSet result = statement.executeQuery(String.format("select * from %s", table))) {
            List<Record> records = new ArrayList<>();
            ResultSetMetaData meta = result.getMetaData();
            int count = lines;
            while(result.next() && count > 0) {
              Record record = new Record();
              for (int i = 1; i < meta.getColumnCount() + 1; ++i) {
                Object object = result.getObject(i);
                if (object instanceof java.sql.Date) {
                  java.sql.Date dt = (java.sql.Date) object;
                  object = dt.toString();
                } else if (object instanceof java.sql.Time) {
                  java.sql.Timestamp dt = (java.sql.Timestamp) object;
                  object = dt.toString();
                } else if (object instanceof java.sql.Time) {
                  java.sql.Time dt = (java.sql.Time) object;
                  object = dt.toString();
                }
                record.add(meta.getColumnName(i), object);
              }
              records.add(record);
              count--;
            }

            String identifier = ServiceUtils.generateMD5(table);
            ws.createWorkspaceMeta(identifier, table);
            String data = gson.toJson(records);
            ws.writeToWorkspace(identifier, WorkspaceDataset.DATA_COL,
                                DataType.RECORDS, data.getBytes(Charsets.UTF_8));

            Map<String, String> properties = new HashMap<>();
            properties.put(PropertyIds.ID, identifier);
            properties.put(PropertyIds.NAME, table);
            properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.DATABASE.getType());
            properties.put(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
            ws.writeProperties(identifier, properties);

            JsonArray values = new JsonArray();
            JsonObject object = new JsonObject();
            object.addProperty(PropertyIds.ID, identifier);
            object.addProperty(PropertyIds.NAME, table);
            object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
            values.add(object);
            response.addProperty("status", HttpURLConnection.HTTP_OK);
            response.addProperty("message", "Success");
            response.addProperty("count", values.size());
            response.add("values", values);
            sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
          }
        }
      });
    } catch (Exception e) {
      error(responder, e.getMessage());
    } finally {
      if (cleanup != null) {
        cleanup.destroy();
      }
    }
  }

  /**
   * Specification for the source.
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   * @param id of the connection.
   * @param table in the database.
   */
  @Path("connections/{id}/tables/{table}/specification")
  @GET
  public void specification(HttpServiceRequest request, final HttpServiceResponder responder,
                            @PathParam("id") String id, @PathParam("table") final String table) {
    JsonObject response = new JsonObject();
    try {
      Connection conn = store.get(id);
      JsonObject value = new JsonObject();
      JsonObject database = new JsonObject();

      Map<String, String> properties = new HashMap<>();
      properties.put("connectionString", (String) conn.getProp("url"));
      properties.put("referenceName", table);
      properties.put("username", (String) conn.getProp("username"));
      properties.put("password", (String) conn.getProp("password"));
      properties.put("importQuery", String.format("SELECT * FROM %s", table));
      properties.put("jdbcPluginName", (String) conn.getProp("name"));
      properties.put("jdbcPluginType", (String) conn.getProp("type"));

      database.add("properties", gson.toJsonTree(properties));
      database.addProperty("name", String.format("Database - %s", table));
      database.addProperty("type", "source");
      value.add("Database", database);

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
   * @param id of the connection to be connected to.
   * @return pair of connection and the driver cleanup.
   */
  private DriverCleanup loadAndExecute(String id, Executor executor) throws Exception {
    Connection connection = store.get(id);
    if (connection == null) {
      throw new IllegalArgumentException(
        String.format(
          "Invalid connection id '%s' specified or connection does not exist.", id)
      );
    }
    return loadAndExecute(connection, executor);
  }

  private DriverCleanup loadAndExecute(Connection connection, Executor executor) throws Exception {
    DriverCleanup cleanup = null;
    String name = connection.getProp("name");
    String classz = connection.getProp("class");
    String url = connection.getProp("url");
    String username = connection.getProp("username");
    String password = connection.getProp("password");

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
      Class<?> driverInfoClass = DBService.class.getClassLoader().loadClass("java.sql.DriverInfo");
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
}
