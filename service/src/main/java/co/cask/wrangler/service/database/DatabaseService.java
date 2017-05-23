package co.cask.wrangler.service.database;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.wrangler.DataPrep;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang.text.StrLookup;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

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

  private final class DriverInfo {
    private String jdbcUrlPattern;
    private String name;

    public DriverInfo(String name, String jdbcUrlPattern) {
      this.name = name;
      this.jdbcUrlPattern = jdbcUrlPattern;
    }

    public String getJdbcUrlPattern() {
      return jdbcUrlPattern;
    }

    public String getName() {
      return name;
    }
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
        if (columns.length == 3) {
          DriverInfo info = new DriverInfo(columns[0], columns[2]);
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
   *        "properties": {
   *          "class": "com.mysql.jdbc.Driver",
   *          "name": "mysql",
   *          "type": "jdbc",
   *          "url": "jdbc:mysql://${hostname}:${port}/${database}?user=${username}&password=${password}"
   *        },
   *
   *      }
   *    ]
   * }
   *
   * @param request HTTP request handler.
   * @param responder HTTP response handler.
   */
  @GET
  @Path("databases/drivers")
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
              object.add("required", required);
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
      ServiceUtils.error(responder, e.getMessage());
    }
  }

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

  @GET
  @Path("connections/{id}/databases/test")
  public void testConnection(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("id") String id) {

  }

  @GET
  @Path("connections/{id}/databases/list")
  public void listTables(HttpServiceRequest request, HttpServiceResponder responder) {

  }

}
