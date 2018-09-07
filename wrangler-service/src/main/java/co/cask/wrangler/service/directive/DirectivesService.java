/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.wrangler.service.directive;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DataSetException;
import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceContext;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.directives.aggregates.DefaultTransientStore;
import co.cask.wrangler.PropertyIds;
import co.cask.wrangler.RequestExtractor;
import co.cask.wrangler.SamplingMethod;
import co.cask.wrangler.ServiceUtils;
import co.cask.wrangler.api.CompileException;
import co.cask.wrangler.api.CompileStatus;
import co.cask.wrangler.api.Compiler;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveConfig;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.GrammarMigrator;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipeSymbol;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.TokenGroup;
import co.cask.wrangler.api.TransientStore;
import co.cask.wrangler.api.parser.Token;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.dataset.workspace.DataType;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceException;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.ConfigDirectiveContext;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.parser.RecipeCompiler;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.registry.UserDirectiveRegistry;
import co.cask.wrangler.sampling.Reservoir;
import co.cask.wrangler.service.connections.ConnectionType;
import co.cask.wrangler.statistics.BasicStatistics;
import co.cask.wrangler.statistics.Statistics;
import co.cask.wrangler.utils.Json2Schema;
import co.cask.wrangler.utils.ObjectSerDe;
import co.cask.wrangler.utils.RecordConvertorException;
import co.cask.wrangler.utils.XPathUtil;
import co.cask.wrangler.validator.ColumnNameValidator;
import co.cask.wrangler.validator.Validator;
import co.cask.wrangler.validator.ValidatorException;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.ximpleware.VTDNav;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import static co.cask.wrangler.ServiceUtils.error;
import static co.cask.wrangler.ServiceUtils.sendJson;
import static co.cask.wrangler.ServiceUtils.success;

/**
 * Service for managing workspaces and also application of directives on to the workspace.
 */
public class DirectivesService extends AbstractHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectivesService.class);
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  public static final String WORKSPACE_DATASET = "workspace";
  private static final String resourceName = ".properties";

  private static final String COLUMN_NAME = "body";
  private static final String RECORD_DELIMITER_HEADER = "recorddelimiter";
  private static final String DELIMITER_HEADER = "delimiter";

  private final Gson gson = new Gson();

  @UseDataSet(WORKSPACE_DATASET)
  private WorkspaceDataset table;

  private DirectiveRegistry composite;

  /**
   * An implementation of HttpService. Stores the context
   * so that it can be used later.
   *
   * @param context the HTTP service runtime context
   * @throws Exception
   */
  @Override
  public void initialize(HttpServiceContext context) throws Exception {
    super.initialize(context);
    composite = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new UserDirectiveRegistry(context)
    );
  }

  /**
   * Closes the resources help by the composite registry.
   */
  @Override
  public void destroy() {
    super.destroy();
    try {
      composite.close();
    } catch (IOException e) {
      // If something bad happens here, you might see a a lot of open file handles.
      LOG.warn("Unable to close the directive registry. You might see increasing number of open file handle.",
               e.getMessage());
    }
  }

  /**
   * Creates a workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : Successfully created workspace 'test'.
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id of workspace to be created.
   * @param name of workspace to be created.
   */
  @PUT
  @Path("workspaces/{id}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("scope") String scope) {
    try {
      if (name == null || name.isEmpty()) {
        name = id;
      }

      if (scope == null || scope.isEmpty()) {
        scope = "default";
      }

      table.createWorkspaceMeta(id, name, scope);
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, id);
      properties.put(PropertyIds.NAME, name);
      table.writeProperties(id, properties);
      success(responder, String.format("Successfully created workspace '%s'", id));
    } catch (WorkspaceException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Lists all workspaces.
   *
   * Following is a response returned
   *
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 4,
   *   "values" : [
   *      { "id" : "ABC", "name" : "body"},
   *      { "id" : "XYZ", "name" : "ws"},
   *      { "id" : "123", "name" : "test"},
   *      { "id" : "UVW", "name" : "foo"}
   *   ]
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @GET
  @Path("workspaces")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("scope") String scope) {
    try {
      if (scope == null || scope.isEmpty()) {
        scope = "default";
      }

      JsonObject response = new JsonObject();
      List<Pair<String,String>> workspaces = table.getWorkspaces();
      JsonArray array = new JsonArray();
      for (Pair<String, String> workspace : workspaces) {
        JsonObject object = new JsonObject();
        object.addProperty("id", workspace.getFirst());
        object.addProperty("name", workspace.getSecond());
        array.add(object);
      }
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (WorkspaceException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Deletes the workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : Successfully deleted workspace 'test'.
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace to deleted.
   */
  @DELETE
  @Path("workspaces/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    try {
      table.deleteWorkspace(id);
      success(responder, String.format("Successfully deleted workspace '%s'", id));
    } catch (WorkspaceException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Deletes the workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : Successfully deleted workspace 'test'.
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param group Group of workspaces.
   */
  @DELETE
  @Path("workspaces/")
  public void deleteGroup(HttpServiceRequest request, HttpServiceResponder responder,
                     @QueryParam("group") String group) {
    try {
      int count = table.deleteGroup(group);
      success(responder, String.format("Successfully deleted %s workspace(s) within group '%s'", count, group));
    } catch (WorkspaceException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Get information about the workspace.
   *
   * Following is the response
   *
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 1,
   *   "values" : [
   *     {
   *       "workspace" : "data",
   *       "created" : 1430202202,
   *       "recipe" : [
   *          "parse-as-csv data ,",
   *          "drop data"
   *       ],
   *       "properties" : {
   *         { "key" : "value"},
   *         { "key" : "value"}
   *       }
   *     }
   *   ]
   * }
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace to deleted.
   */
  @GET
  @Path("workspaces/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    JsonObject response = new JsonObject();
    JsonArray values = new JsonArray();
    try {
      byte[] bytes = table.getData(id, WorkspaceDataset.NAME_COL);
      String name = "";
      if (bytes != null) {
        name = Bytes.toString(bytes);
      }
      String data = table.getData(id, WorkspaceDataset.REQUEST_COL, DataType.TEXT);
      JsonObject req = new JsonObject();
      if (data != null) {
        req = (JsonObject) new JsonParser().parse(data);
      }
      Map<String, String> properties = table.getProperties(id);
      JsonObject prop = (JsonObject) GSON.toJsonTree(properties);

      prop.addProperty("name", name);
      prop.addProperty("id", name);
      req.add("properties", merge(req.getAsJsonObject("properties"), prop));
      values.add(req);
      response.addProperty("count", 1);
      response.add("values", values);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (WorkspaceException | JSONException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Merges two {@link JsonObject} into a single object. If the keys are
   * overlapping, then the second JsonObject keys will overwrite the first.
   *
   * @param first {@link JsonObject}
   * @param second  {@link JsonObject}
   * @return merged {@link JsonObject}
   */
  private JsonObject merge(JsonObject first, JsonObject second) {
    JsonObject merged = new JsonObject();
    if (first != null && !first.isJsonNull()) {
      for(Map.Entry<String, JsonElement> entry: first.entrySet()) {
        merged.add(entry.getKey(), entry.getValue());
      }
    }
    if (second != null && !second.isJsonNull()) {
      for(Map.Entry<String, JsonElement> entry: second.entrySet()) {
        merged.add(entry.getKey(), entry.getValue());
      }
    }
    return merged;
  }

  /**
   * Upload data to the workspace, the workspace is created automatically on fly.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @POST
  @Path("workspaces")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder) {

    try {
      String name = request.getHeader(PropertyIds.FILE_NAME);
      String id = ServiceUtils.generateMD5(name);

      // if workspace doesn't exist, then we create the workspace before
      // adding data to the workspace.
      if (!table.hasWorkspace(id)) {
        table.createWorkspaceMeta(id, name);
      }

      RequestExtractor handler = new RequestExtractor(request);

      // For back-ward compatibility, we check if there is delimiter specified
      // using 'recorddelimiter' or 'delimiter'
      String delimiter = handler.getHeader(RECORD_DELIMITER_HEADER, "\\u001A");
      delimiter = handler.getHeader(DELIMITER_HEADER, delimiter);

      // Extract charset, if not specified, default it to UTF-8.
      String charset = handler.getHeader(RequestExtractor.CHARSET_HEADER, "UTF-8");

      // Get content type - application/data-prep, application/octet-stream or text/plain.
      String contentType = handler.getHeader(RequestExtractor.CONTENT_TYPE_HEADER, "application/data-prep");

      // Extract content.
      byte[] content = handler.getContent();
      if (content == null) {
        error(responder, "Body not present, please post the file containing the records to be wrangled.");
        return;
      }

      // Depending on content type, load data.
      DataType type = DataType.fromString(contentType);
      switch(type) {
        case TEXT: {
          // Convert the type into unicode.
          String body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.TEXT, Bytes.toBytes(body));
          break;
        }

        case RECORDS: {
          delimiter = StringEscapeUtils.unescapeJava(delimiter);
          String body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          List<Row> rows = new ArrayList<>();
          for (String line : body.split(delimiter)) {
            rows.add(new Row(COLUMN_NAME, line));
          }
          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          byte[] bytes = serDe.toByteArray(rows);
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, bytes);
          break;
        }

        case BINARY: {
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.BINARY, content);
          break;
        }

        default: {
          error(responder, "Invalid content type. Supports text/plain, application/octet-stream " +
            "and application/data-prep");
          break;
        }
      }

      // Write properties for workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, id);
      properties.put(PropertyIds.NAME, name);
      properties.put(PropertyIds.DELIMITER, delimiter);
      properties.put(PropertyIds.CHARSET, charset);
      properties.put(PropertyIds.CONTENT_TYPE, contentType);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.UPLOAD.getType());
      table.writeProperties(id, properties);

      JsonArray array = new JsonArray();
      JsonObject object = (JsonObject) GSON.toJsonTree(properties);
      object.addProperty(PropertyIds.SAMPLER_TYPE, SamplingMethod.NONE.getMethod());
      array.add(object);

      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (WorkspaceException | IOException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Upload data to the workspace.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Upload data to the workspace.
   */
  @POST
  @Path("workspaces/{id}/upload")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    try {
      RequestExtractor handler = new RequestExtractor(request);

      // For back-ward compatibility, we check if there is delimiter specified
      // using 'recorddelimiter' or 'delimiter'
      String delimiter = handler.getHeader(RECORD_DELIMITER_HEADER, "\\u001A");
      delimiter = handler.getHeader(DELIMITER_HEADER, delimiter);

      // Extract charset, if not specified, default it to UTF-8.
      String charset = handler.getHeader(RequestExtractor.CHARSET_HEADER, "UTF-8");

      // Get content type - application/data-prep, application/octet-stream or text/plain.
      String contentType = handler.getHeader(RequestExtractor.CONTENT_TYPE_HEADER, "application/data-prep");

      // Extract content.
      byte[] content = handler.getContent();
      if (content == null) {
        error(responder, "Body not present, please post the file containing the records to be wrangle.");
        return;
      }

      // Depending on content type, load data.
      DataType type = DataType.fromString(contentType);
      switch(type) {
        case TEXT: {
          // Convert the type into unicode.
          String body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.TEXT, Bytes.toBytes(body));
          break;
        }

        case RECORDS: {
          delimiter = StringEscapeUtils.unescapeJava(delimiter);
          String body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          List<Row> rows = new ArrayList<>();
          for (String line : body.split(delimiter)) {
            rows.add(new Row(id, line));
          }
          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          byte[] bytes = serDe.toByteArray(rows);
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.RECORDS, bytes);
          break;
        }

        case BINARY: {
          table.writeToWorkspace(id, WorkspaceDataset.DATA_COL, DataType.BINARY, content);
          break;
        }

        default: {
          error(responder, "Invalid content type. Supports text/plain, application/octet-stream " +
            "and application/data-prep");
          break;
        }
      }

      // Write properties for workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.DELIMITER, delimiter);
      properties.put(PropertyIds.CHARSET, charset);
      properties.put(PropertyIds.CONTENT_TYPE, contentType);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.UPLOAD.getType());
      table.writeProperties(id, properties);

      success(responder, String.format("Successfully uploaded data to workspace '%s'", id));
    } catch (WorkspaceException | IOException e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Executes the directives on the record stored in the workspace.
   *
   * Following is the response from this request
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "count" : 2,
   *   "header" : [ "a", "b", "c", "d" ],
   *   "value" : [
   *     { record 1},
   *     { record 2}
   *   ]
   * }
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   * @param id workspace in which the directives are executed.
   */
  @POST
  @Path("workspaces/{id}/execute")
  public void execute(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("id") String id) {
    try {
      RequestExtractor handler = new RequestExtractor(request);
      Request user = handler.getContent("UTF-8", Request.class);
      user.getRecipe().setPragma(addLoadablePragmaDirectives(user));

      final int limit = user.getSampling().getLimit();
      List<Row> rows = executeDirectives(id, user, new Function<List<Row>, List<Row>>() {
        @Nullable
        @Override
        public List<Row> apply(@Nullable List<Row> records) {
          int min = Math.min(records.size(), limit);
          return records.subList(0, min);
        }
      });

      JsonArray values = new JsonArray();
      JsonArray headers = new JsonArray();
      JsonObject types = new JsonObject();
      Set<String> header = new HashSet<>();

      // Iterate through all the new rows.
      for (Row row : rows) {
        JsonObject value = new JsonObject();
        // If output array has more than return result values, we terminate.
        if (values.size() >= user.getWorkspace().getResults()) {
          break;
        }

        // Iterate through all the fields of the row.
        List<Pair<String, Object>> fields = row.getFields();
        for (Pair<String, Object> field : fields) {
          // If not present in header, add it to header.
          if (!header.contains(field.getFirst())) {
            headers.add(new JsonPrimitive(field.getFirst()));
            header.add(field.getFirst());
          }
          Object object = field.getSecond();

          if (object != null) {
            types.addProperty(field.getFirst(), object.getClass().getSimpleName());
            if ((object.getClass().getMethod("toString").getDeclaringClass() != Object.class)) {
              value.addProperty(field.getFirst(), object.toString());
            } else {
              value.addProperty(field.getFirst(), "Non-displayable object");
            }
          } else {
            value.add(field.getFirst(), JsonNull.INSTANCE);
          }
        }
        values.add(value);
      }

      // Save the recipes being executed.
      table.updateWorkspace(id, WorkspaceDataset.REQUEST_COL, GSON.toJson(user));

      JsonObject response = new JsonObject();
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", values.size());
      response.add("header", headers); // TODO: Remove this later. 
      response.add("types", types);
      response.add("directives", gson.toJsonTree(user.getRecipe().getDirectives()));
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (JsonParseException e) {
      LOG.warn(e.getMessage(), e);
      error(responder, "Issue parsing request. " + e.getMessage());
    } catch (DirectiveParseException e) {
      LOG.warn(e.getMessage(), e);
      error(responder, e.getMessage());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      error(responder, e.getMessage());
    }
  }

  /**
   * Automatically adds a load-directives pragma to the list of directives.
   */
  private String addLoadablePragmaDirectives(Request request) {
    StringBuilder sb = new StringBuilder();
    // Validate the DSL by compiling the DSL. In case of macros being
    // specified, the compilation will them at this phase.
    Compiler compiler = new RecipeCompiler();
    try {
      // Compile the directive extracting the loadable plugins (a.k.a
      // Directives in this context).
      CompileStatus status = compiler.compile(new MigrateToV2(request.getRecipe().getDirectives()).migrate());
      RecipeSymbol symbols = status.getSymbols();
      Iterator<TokenGroup> iterator = symbols.iterator();
      List<String> userDirectives = new ArrayList<>();
      while(iterator.hasNext()) {
        TokenGroup next = iterator.next();
        if (next == null || next.size() < 1) {
          continue;
        }
        Token token = next.get(0);
        if (token.type() == TokenType.DIRECTIVE_NAME) {
          String directive = (String) token.value();
          try {
            DirectiveInfo.Scope scope = composite.get(directive).scope();
            if (scope == DirectiveInfo.Scope.USER) {
              userDirectives.add(directive);
            }
          } catch (DirectiveLoadException e) {
            // no-op.
          }
        }
      }
      if (userDirectives.size() > 0) {
        sb.append("#pragma load-directives ");
        String directives = StringUtils.join(userDirectives, ",");
        sb.append(directives).append(";");
        return sb.toString();
      }
    } catch (CompileException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    } catch (DirectiveParseException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    return null;
  }

  /**
   * Summarizes the workspace by running directives.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace data to be summarized.
   */
  @POST
  @Path("workspaces/{id}/summary")
  public void summary(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("id") String id) {
    try {
      RequestExtractor handler = new RequestExtractor(request);
      Request user = handler.getContent("UTF-8", Request.class);
      final int limit = user.getSampling().getLimit();
      List<Row> rows = executeDirectives(id, user, new Function<List<Row>, List<Row>>() {
        @Nullable
        @Override
        public List<Row> apply(@Nullable List<Row> records) {
          int min = Math.min(records.size(), limit);
          return records.subList(0, min);
        }
      });

      // Final response object.
      JsonObject response = new JsonObject();
      JsonObject result = new JsonObject();

      // Validate Column names.
      Validator<String> validator = new ColumnNameValidator();
      validator.initialize();

      // Iterate through columns to value a set
      Set<String> uniqueColumns = new HashSet<>();
      for (Row row : rows) {
        for (int i = 0; i < row.length(); ++i) {
          uniqueColumns.add(row.getColumn(i));
        }
      }

      JsonObject columnValidationResult = new JsonObject();
      for (String name : uniqueColumns) {
        JsonObject columnResult = new JsonObject();
        try {
          validator.validate(name);
          columnResult.addProperty("valid", true);
        } catch (ValidatorException e) {
          columnResult.addProperty("valid", false);
          columnResult.addProperty("message", e.getMessage());
        }
        columnValidationResult.add(name, columnResult);
      }

      result.add("validation", columnValidationResult);

      // Generate General and Type related Statistics for each column.
      Statistics statsGenerator = new BasicStatistics();
      Row summary = statsGenerator.aggregate(rows);

      Row stats = (Row) summary.getValue("stats");
      Row types = (Row) summary.getValue("types");

      // Serialize the results into JSON.
      List<Pair<String, Object>> fields = stats.getFields();
      JsonObject statistics = new JsonObject();
      for (Pair<String, Object> field : fields) {
        List<Pair<String, Double>> values = (List<Pair<String, Double>>) field.getSecond();
        JsonObject v = new JsonObject();
        JsonObject o = new JsonObject();
        for (Pair<String, Double> value : values) {
          o.addProperty(value.getFirst(), value.getSecond().floatValue()*100);
        }
        v.add("general", o);
        statistics.add(field.getFirst(), v);
      }

      fields = types.getFields();
      for (Pair<String, Object> field : fields) {
        List<Pair<String, Double>> values = (List<Pair<String, Double>>) field.getSecond();
        JsonObject v = new JsonObject();
        JsonObject o = new JsonObject();
        for (Pair<String, Double> value : values) {
          o.addProperty(value.getFirst(), value.getSecond().floatValue()*100);
        }
        v.add("types", o);
        JsonObject object = (JsonObject) statistics.get(field.getFirst());
        if (object == null) {
          statistics.add(field.getFirst(), v);
        } else {
          object.add("types", o);
        }
      }

      // Put the statistics along with validation rules.
      result.add("statistics", statistics);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", 2);
      response.add("values", result);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  @POST
  @Path("workspaces/{id}/schema")
  public void schema(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("id") String id) {
    try {
      RequestExtractor handler = new RequestExtractor(request);
      Request user = handler.getContent("UTF-8", Request.class);
      final int limit = user.getSampling().getLimit();
      List<Row> rows = executeDirectives(id, user, new Function<List<Row>, List<Row>>() {
        @Nullable
        @Override
        public List<Row> apply(@Nullable List<Row> records) {
          int min = Math.min(records.size(), limit);
          return records.subList(0, min);
        }
      });

      // generate a schema based upon the first record
      Json2Schema json2Schema = new Json2Schema();
      try {
        Schema schema = json2Schema.toSchema("record", createUberRecord(rows));
        if (schema.getType() != Schema.Type.RECORD) {
          schema = Schema.recordOf("array", Schema.Field.of("value", schema));
        }

        String schemaJson = GSON.toJson(schema);
        // the current contract with the UI is not to pass the
        // entire schema string, but just the fields.
        String fieldsJson = new JsonParser().parse(schemaJson)
                                    .getAsJsonObject()
                                    .get("fields").toString();
        sendJson(responder, HttpURLConnection.HTTP_OK, fieldsJson);
      } catch (RecordConvertorException e) {
        error(responder, "There was a problem in generating schema for the record. " + e.getMessage());
        return;
      }
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Generates the capability matrix, with versions and build number.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @GET
  @Path("info")
  public void capabilities(HttpServiceRequest request, HttpServiceResponder responder) {
    JsonArray values = new JsonArray();

    ClassLoader loader = Thread.currentThread().getContextClassLoader();
    Properties props = new Properties();
    try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
      props.load(resourceStream);
    } catch (IOException e) {
      error(responder, "There was problem reading the capability matrix. " +
        "Please check the environment to ensure you have right verions of jar." + e.getMessage());
      return;
    }

    JsonObject object = new JsonObject();
    for(String key : props.stringPropertyNames()) {
      String value = props.getProperty(key);
      object.addProperty(key, value);
    }
    values.add(object);

    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "Success");
    response.addProperty("count", values.size());
    response.add("values", values);
    sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
  }

  /**
   * Executes the directives on the record stored in the workspace and returns the possible xpaths for a
   * particular column.
   *
   * Following is the response from this request
   * {
   *   "status" : 200,
   *   "message" : "Success",
   *   "xmlpaths" : [
   *     /ns1:XYZ/ns1:Summary
   *     /ns1:XYZ/ns1:ChangeIndicators/ns1:Itinerary
   *     /ns1:XYZ/ns1:ChangeIndicators/ns1:Name
   *   ]
   * }
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   * @param ws workspace in which the directives are executed.
   */
  @GET
  @Path("workspaces/{workspace}/xpaths/{column}")
  public void getXPaths(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("workspace") String ws,
                        @PathParam("column") String column) {
    try {

      // Read the request body
      RequestExtractor handler = new RequestExtractor(request);
      Request reqBody = handler.getContent("UTF-8", Request.class);
      if (reqBody == null) {
        error(responder, "Request is empty. Please check if the request is sent as HTTP POST body.");
        return;
      }

      final int limit = reqBody.getSampling().getLimit();
      List<Row> newRows = executeDirectives(ws, reqBody, new Function<List<Row>, List<Row>>() {
        @Nullable
        @Override
        public List<Row> apply(@Nullable List<Row> records) {
          int min = Math.min(records.size(), limit);
          return Lists.newArrayList(new Reservoir<Row>(min).sample(records.iterator()));
        }
      });


      Row firstRow = newRows.get(0);
      Object columnValue = firstRow.getValue(column);
      if (!(columnValue instanceof VTDNav)) {
        error(responder, String.format("Column '%s' is not parsed as XML.", column));
        return;
      }

      Set<String> xmlPaths = XPathUtil.getXMLPaths((VTDNav) columnValue);

      JSONObject response = new JSONObject();
      response.put("status", HttpURLConnection.HTTP_OK);
      response.put("message", "Success");
      response.put("count", xmlPaths.size());
      response.put("values", xmlPaths);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (JsonParseException e) {
      error(responder, "Issue parsing request. " + e.getMessage());
    } catch (DataSetException e) {
      error(responder, e.getMessage());
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * This REST API returns an array of all the directives, their usage and description.
   *
   * Following is the response of this call.
   * {
   *   "status": "OK",
   *   "message": "Success",
   *   "count" : 10,
   *   "values" : [
   *      {
   *        "directive" : "parse-as-csv",
   *        "usage" : "parse-as-csv <column> <delimiter> <skip-empty-row>",
   *        "description" : "Parses as CSV ..."
   *      },
   *      ...
   *   ]
   * }
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   */
  @GET
  @Path("usage")
  public void usage(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      DirectiveConfig config = table.getConfig();
      Map<String, List<String>> aliases = config.getReverseAlias();
      JsonObject response = new JsonObject();
      JsonArray values = new JsonArray();

      int count = 0;
      Iterator<DirectiveInfo> iterator = composite.iterator();
      while(iterator.hasNext()) {
        JsonObject usage = new JsonObject();
        DirectiveInfo directive = iterator.next();
        usage.addProperty("directive", directive.name());
        usage.addProperty("usage", directive.usage());
        usage.addProperty("description", directive.description());
        usage.addProperty("excluded", config.isExcluded(directive.name()));
        usage.addProperty("alias", false);
        usage.addProperty("scope", directive.scope().name());
        usage.add("arguments", directive.definition().toJson());
        usage.add("categories", GSON.toJsonTree(directive.categories()));
        values.add(usage);

        // For this directive we find all aliases and add them to the
        // description.
        if (aliases.containsKey(directive)) {
          List<String> list = aliases.get(directive);
          for(String alias : list) {
            JsonObject aliasUsage = new JsonObject();
            aliasUsage.addProperty("directive", alias);
            aliasUsage.addProperty("usage", directive.usage());
            aliasUsage.addProperty("description", directive.description());
            aliasUsage.addProperty("excluded", config.isExcluded(alias));
            aliasUsage.addProperty("alias", true);
            aliasUsage.addProperty("scope", directive.scope().name());
            aliasUsage.add("arguments", directive.definition().toJson());
            aliasUsage.add("categories", GSON.toJsonTree(directive.categories()));
            values.add(aliasUsage);
            count++;
          }
        }
        count++;
      }

      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", count);
      response.add("values", values);
      sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      error(responder, e.getMessage());
    }
  }

  /**
   * This HTTP endpoint is used to retrieve artifacts that include plugins
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("artifacts")
  public void artifacts(HttpServiceRequest request, HttpServiceResponder responder) {
    JsonObject response = new JsonObject();
    JsonArray values = new JsonArray();
    try {
      List<ArtifactInfo> artifacts = getContext().listArtifacts();
      for(ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        for (PluginClass plugin : plugins) {
          if (Directive.Type.equalsIgnoreCase(plugin.getType())) {
            JsonObject object = new JsonObject();
            object.addProperty("name", artifact.getName());
            object.addProperty("version", artifact.getVersion());
            object.addProperty("scope", artifact.getScope().name());
            object.add("properties", GSON.toJsonTree(artifact.getProperties()));
            values.add(object);
            break;
          }
        }
      }
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
   * This HTTP endpoint is used to retrieve plugins that are
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("directives")
  public void directives(HttpServiceRequest request, HttpServiceResponder responder) {
    JsonObject response = new JsonObject();
    JsonArray values = new JsonArray();
    try {
      List<ArtifactInfo> artifacts = getContext().listArtifacts();
      for(ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        JsonObject object = new JsonObject();
        object.addProperty("name", artifact.getName());
        object.addProperty("version", artifact.getVersion());
        object.addProperty("scope", artifact.getScope().name());
        for (PluginClass plugin : plugins) {
          if (Directive.Type.equalsIgnoreCase(plugin.getType())) {
            JsonObject directive = new JsonObject();
            directive.addProperty("name", plugin.getName());
            directive.addProperty("description", plugin.getDescription());
            directive.addProperty("type", plugin.getType());
            directive.addProperty("class", plugin.getClassName());
            directive.add("artifact", object);
            values.add(directive);
          }
        }
      }
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
   * This HTTP endpoint is used to reload the plugins that are
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("directives/reload")
  public void directivesReload(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      composite.reload();
      success(responder, "Successfully reloaded all user defined directives.");
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Extracts the charsets supported.
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   */
  @GET
  @Path("charsets")
  public void charsets(HttpServiceRequest request, HttpServiceResponder responder) {
    Set<String> charsets = Charset.availableCharsets().keySet();
    JsonObject response = new JsonObject();
    response.addProperty("status", HttpURLConnection.HTTP_OK);
    response.addProperty("message", "success");
    response.addProperty("count", charsets.size());
    JsonArray array = new JsonArray();
    for (String charset : charsets) {
      array.add(new JsonPrimitive(charset));
    }
    response.add("values", array);
    sendJson(responder, HttpURLConnection.HTTP_OK, response.toString());
  }

  /**
   * Updates the configuration.
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   */
  @POST
  @Path("config")
  public void uploadConfig(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      // Read the request body
      RequestExtractor handler = new RequestExtractor(request);
      DirectiveConfig config = handler.getContent("UTF-8", DirectiveConfig.class);
      if (config == null) {
        error(responder, "Config is empty. Please check if the request is sent as HTTP POST body.");
        return;
      }
      table.updateConfig(config);
      success(responder, "Successfully updated configuration.");
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Updates the configuration.
   *
   * @param request to gather information of the request.
   * @param responder to respond to the service request.
   */
  @GET
  @Path("config")
  public void getConfig(HttpServiceRequest request, HttpServiceResponder responder) {
    try {
      String response = table.getConfigString();
      sendJson(responder, HttpURLConnection.HTTP_OK, response);
    } catch (Exception e) {
      error(responder, e.getMessage());
    }
  }

  /**
   * Creates a uber record after iterating through all rows.
   *
   * @param rows list of all rows.
   * @return A single record will rows merged across all columns.
   */
  private static Row createUberRecord(List<Row> rows) {
    Row uber = new Row();
    for (Row row : rows) {
      for (int i = 0; i < row.length(); ++i) {
        Object o = row.getValue(i);
        if (o != null) {
          int idx = uber.find(row.getColumn(i));
          if (idx == -1) {
            uber.add(row.getColumn(i), o);
          }
        }
      }
    }
    return uber;
  }

  /**
   * Converts the data in workspace into records.
   *
   * @param id name of the workspace from which the records are generated.
   * @return list of records.
   * @throws WorkspaceException thrown when there is issue retrieving data.
   */
  private List<Row> fromWorkspace(String id) throws WorkspaceException {
    DataType type = table.getType(id);
    List<Row> rows = new ArrayList<>();

    if (type == null) {
      throw new WorkspaceException("Workspace you are currently working on seemed to have " +
                                     "disappeared, please reload the data.");
    }

    switch(type) {
      case TEXT: {
        String data = table.getData(id, WorkspaceDataset.DATA_COL, DataType.TEXT);
        if (data != null) {
          rows.add(new Row("body", data));
        }
        break;
      }

      case BINARY: {
        byte[] data = table.getData(id, WorkspaceDataset.DATA_COL, DataType.BINARY);
        if (data != null) {
          rows.add(new Row("body", data));
        }
        break;
      }

      case RECORDS: {
        rows = table.getData(id, WorkspaceDataset.DATA_COL, DataType.RECORDS);
        break;
      }
    }
    return rows;
  }

  /**
   * Executes directives by extracting them from request.
   *
   * @param id data to be used for executing directives.
   * @param user request passed on http.
   * @param sample sampling function.
   * @return records generated from the directives.
   */
  private List<Row> executeDirectives(String id, @Nullable Request user,
                                      Function<List<Row>, List<Row>> sample)
    throws Exception {
    if (user == null) {
      throw new Exception("Request is empty. Please check if the request is sent as HTTP POST body.");
    }

    TransientStore store = new DefaultTransientStore();
    // Extract rows from the workspace.
    List<Row> rows = fromWorkspace(id);
    // Execute the pipeline.
    ExecutorContext context = new ServicePipelineContext(ExecutorContext.Environment.SERVICE,
                                                         getContext(),
                                                         store);
    RecipePipelineExecutor executor = new RecipePipelineExecutor();
    if (user.getRecipe().getDirectives().size() > 0) {
      GrammarMigrator migrator = new MigrateToV2(user.getRecipe().getDirectives());
      String migrate = migrator.migrate();
      RecipeParser recipe = new GrammarBasedParser(migrate, composite);
      recipe.initialize(new ConfigDirectiveContext(table.getConfigString()));
      executor.initialize(recipe, context);
      rows = executor.execute(sample.apply(rows));
      executor.destroy();
    }
    return rows;
  }
}
