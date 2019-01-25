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

package co.cask.wrangler.service.directive;

import co.cask.cdap.api.artifact.ArtifactInfo;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginClass;
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
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveParseException;
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
import co.cask.wrangler.dataset.workspace.Workspace;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.dataset.workspace.WorkspaceMeta;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.ConfigDirectiveContext;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.parser.RecipeCompiler;
import co.cask.wrangler.proto.BadRequestException;
import co.cask.wrangler.proto.NamespacedId;
import co.cask.wrangler.proto.Request;
import co.cask.wrangler.proto.ServiceResponse;
import co.cask.wrangler.proto.WorkspaceIdentifier;
import co.cask.wrangler.proto.connection.ConnectionType;
import co.cask.wrangler.proto.workspace.ColumnStatistics;
import co.cask.wrangler.proto.workspace.ColumnValidationResult;
import co.cask.wrangler.proto.workspace.DirectiveArtifact;
import co.cask.wrangler.proto.workspace.DirectiveDescriptor;
import co.cask.wrangler.proto.workspace.DirectiveExecutionResponse;
import co.cask.wrangler.proto.workspace.DirectiveUsage;
import co.cask.wrangler.proto.workspace.WorkspaceInfo;
import co.cask.wrangler.proto.workspace.WorkspaceSummaryResponse;
import co.cask.wrangler.proto.workspace.WorkspaceValidationResult;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.DirectiveInfo;
import co.cask.wrangler.registry.DirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import co.cask.wrangler.registry.UserDirectiveRegistry;
import co.cask.wrangler.service.common.AbstractWranglerHandler;
import co.cask.wrangler.statistics.BasicStatistics;
import co.cask.wrangler.statistics.Statistics;
import co.cask.wrangler.utils.Json2Schema;
import co.cask.wrangler.utils.ObjectSerDe;
import co.cask.wrangler.validator.ColumnNameValidator;
import co.cask.wrangler.validator.Validator;
import co.cask.wrangler.validator.ValidatorException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Service for managing workspaces and also application of directives on to the workspace.
 */
public class DirectivesHandler extends AbstractWranglerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DirectivesHandler.class);
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final String resourceName = ".properties";

  private static final String COLUMN_NAME = "body";
  private static final String RECORD_DELIMITER_HEADER = "recorddelimiter";
  private static final String DELIMITER_HEADER = "delimiter";

  private DirectiveRegistry composite;

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

  @PUT
  @Path("workspaces/{id}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    create(request, responder, getContext().getNamespace(), id, name, scope);
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
  @Path("contexts/{context}/workspaces/{id}")
  public void create(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, () -> {
      String workspaceName = name == null || name.isEmpty() ? id : name;

      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, id);
      properties.put(PropertyIds.NAME, workspaceName);
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(new NamespacedId(namespace, id), workspaceName)
        .setScope(scope)
        .setProperties(properties)
        .build();
      ws.writeWorkspaceMeta(workspaceMeta);
      return new ServiceResponse<Void>(String.format("Successfully created workspace '%s'", id));
    });
  }

  @GET
  @Path("workspaces")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @QueryParam("scope") @DefaultValue("default") String scope) {
    list(request, responder, getContext().getNamespace(), scope);
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
  @Path("contexts/{context}/workspaces")
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace, @QueryParam("scope") @DefaultValue("default") String scope) {
    respond(request, responder, namespace, () -> {
      List<WorkspaceIdentifier> workspaces = ws.listWorkspaces(namespace, scope);
      return new ServiceResponse<>(workspaces);
    });
  }

  @DELETE
  @Path("workspaces/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    delete(request, responder, getContext().getNamespace(), id);
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
  @Path("contexts/{context}/workspaces/{id}")
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      ws.deleteWorkspace(new NamespacedId(namespace, id));
      return new ServiceResponse<Void>(String.format("Successfully deleted workspace '%s'", id));
    });
  }

  @DELETE
  @Path("workspaces/")
  public void deleteGroup(HttpServiceRequest request, HttpServiceResponder responder,
                          @QueryParam("group") String group) {
    deleteGroup(request, responder, getContext().getNamespace(), group);
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
  @Path("contexts/{context}/workspaces/")
  public void deleteGroup(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace, @QueryParam("group") String group) {
    respond(request, responder, namespace, () -> {
      int count = ws.deleteScope(namespace, group);
      return new ServiceResponse<Void>(String.format("Successfully deleted %s workspace(s) within group '%s'",
                                                     count, group));
    });
  }

  @GET
  @Path("workspaces/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("id") String id) {
    get(request, responder, getContext().getNamespace(), id);
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
  @Path("contexts/{context}/workspaces/{id}")
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      Workspace workspace = ws.getWorkspace(new NamespacedId(namespace, id));
      String name = workspace.getName();
      Request workspaceReq = workspace.getRequest();
      JsonObject req = new JsonObject();
      if (workspaceReq != null) {
        req = (JsonObject) GSON.toJsonTree(workspaceReq);
      }
      Map<String, String> properties = workspace.getProperties();
      JsonObject prop = workspaceReq == null ? new JsonObject() : (JsonObject) GSON.toJsonTree(properties);

      prop.addProperty("name", name);
      prop.addProperty("id", name);
      req.add("properties", merge(req.getAsJsonObject("properties"), prop));
      return new ServiceResponse<>(req);
    });
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
      for (Map.Entry<String, JsonElement> entry: first.entrySet()) {
        merged.add(entry.getKey(), entry.getValue());
      }
    }
    if (second != null && !second.isJsonNull()) {
      for (Map.Entry<String, JsonElement> entry: second.entrySet()) {
        merged.add(entry.getKey(), entry.getValue());
      }
    }
    return merged;
  }

  @POST
  @Path("workspaces")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder) {
    upload(request, responder, getContext().getNamespace());
  }

  /**
   * Upload data to the workspace, the workspace is created automatically on fly.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @POST
  @Path("contexts/{context}/workspaces")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      String name = request.getHeader(PropertyIds.FILE_NAME);
      if (name == null) {
        throw new BadRequestException("Name must be provided in the 'file' header");
      }
      NamespacedId id = new NamespacedId(namespace, ServiceUtils.generateMD5(name));

      // if workspace doesn't exist, then we create the workspace before
      // adding data to the workspace.
      if (!ws.hasWorkspace(id)) {
        ws.writeWorkspaceMeta(WorkspaceMeta.builder(id, name).build());
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
        throw new BadRequestException("Body not present, please post the file containing the records to be wrangled.");
      }

      // Depending on content type, load data.
      DataType type = DataType.fromString(contentType);
      if (type == null) {
        throw new BadRequestException("Invalid content type. Must be 'text/plain', 'application/octet-stream' " +
                                        "or 'application/data-prep'");
      }
      switch (type) {
        case TEXT:
          // Convert the type into unicode.
          String body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          ws.updateWorkspaceData(id, DataType.TEXT, Bytes.toBytes(body));
          break;

        case RECORDS:
          delimiter = StringEscapeUtils.unescapeJava(delimiter);
          body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          List<Row> rows = new ArrayList<>();
          for (String line : body.split(delimiter)) {
            rows.add(new Row(COLUMN_NAME, line));
          }
          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          byte[] bytes = serDe.toByteArray(rows);
          ws.updateWorkspaceData(id, DataType.RECORDS, bytes);
          break;

        case BINARY:
          ws.updateWorkspaceData(id, DataType.BINARY, content);
          break;
      }

      // Write properties for workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.ID, id.getId());
      properties.put(PropertyIds.NAME, name);
      properties.put(PropertyIds.DELIMITER, delimiter);
      properties.put(PropertyIds.CHARSET, charset);
      properties.put(PropertyIds.CONTENT_TYPE, contentType);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.UPLOAD.getType());
      ws.updateWorkspaceProperties(id, properties);

      WorkspaceInfo workspaceInfo = new WorkspaceInfo(id.getId(), name, delimiter, charset, contentType,
                                                      ConnectionType.UPLOAD.getType(), SamplingMethod.NONE.getMethod());
      return new ServiceResponse<>(workspaceInfo);
    });
  }


  @POST
  @Path("workspaces/{id}/upload")
  public void uploadData(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("id") String id) {
    uploadData(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Upload data to the workspace.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Upload data to the workspace.
   */
  @POST
  @Path("contexts/{context}/workspaces/{id}/upload")
  public void uploadData(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
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
        throw new BadRequestException("Body not present, please post the file containing the records to be wrangled.");
      }

      // Depending on content type, load data.
      DataType type = DataType.fromString(contentType);
      if (type == null) {
        throw new BadRequestException("Invalid content type. Must be 'text/plain', 'application/octet-stream' " +
                                        "or 'application/data-prep'");
      }
      NamespacedId namespaceId = new NamespacedId(namespace, id);
      switch (type) {
        case TEXT:
          // Convert the type into unicode.
          String body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          ws.updateWorkspaceData(namespaceId, DataType.TEXT, Bytes.toBytes(body));
          break;

        case RECORDS:
          delimiter = StringEscapeUtils.unescapeJava(delimiter);
          body = Charset.forName(charset).decode(ByteBuffer.wrap(content)).toString();
          List<Row> rows = new ArrayList<>();
          for (String line : body.split(delimiter)) {
            rows.add(new Row(id, line));
          }
          ObjectSerDe<List<Row>> serDe = new ObjectSerDe<>();
          byte[] bytes = serDe.toByteArray(rows);
          ws.updateWorkspaceData(namespaceId, DataType.RECORDS, bytes);
          break;

        case BINARY:
          ws.updateWorkspaceData(namespaceId, DataType.BINARY, content);
          break;
      }

      // Write properties for workspace.
      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.DELIMITER, delimiter);
      properties.put(PropertyIds.CHARSET, charset);
      properties.put(PropertyIds.CONTENT_TYPE, contentType);
      properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.UPLOAD.getType());
      ws.updateWorkspaceProperties(namespaceId, properties);

      return new ServiceResponse<Void>(String.format("Successfully uploaded data to workspace '%s'", id));
    });
  }

  @POST
  @Path("workspaces/{id}/execute")
  public void execute(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("id") String id) {
    execute(request, responder, getContext().getNamespace(), id);
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
  @Path("contexts/{context}/workspaces/{id}/execute")
  public void execute(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      try {
        RequestExtractor handler = new RequestExtractor(request);
        Request directiveRequest = handler.getContent("UTF-8", Request.class);
        if (directiveRequest == null) {
          throw new BadRequestException("Request body is empty.");
        }
        directiveRequest.getRecipe().setPragma(addLoadablePragmaDirectives(namespace, directiveRequest));

        int limit = directiveRequest.getSampling().getLimit();
        NamespacedId namespacedId = new NamespacedId(namespace, id);
        List<Row> rows = executeDirectives(namespacedId, directiveRequest, records -> {
          if (records == null) {
            return Collections.emptyList();
          }
          int min = Math.min(records.size(), limit);
          return records.subList(0, min);
        });

        List<Map<String, Object>> values = new ArrayList<>(rows.size());
        Map<String, String> types = new HashMap<>();
        Set<String> headers = new LinkedHashSet<>();

        // Iterate through all the new rows.
        for (Row row : rows) {
          // If output array has more than return result values, we terminate.
          if (values.size() >= directiveRequest.getWorkspace().getResults()) {
            break;
          }

          Map<String, Object> value = new HashMap<>(row.length());

          // Iterate through all the fields of the row.
          for (Pair<String, Object> field : row.getFields()) {
            String fieldName = field.getFirst();
            headers.add(fieldName);
            Object object = field.getSecond();

            if (object != null) {
              types.put(fieldName, object.getClass().getSimpleName());
              if ((object.getClass().getMethod("toString").getDeclaringClass() != Object.class)) {
                value.put(fieldName, object.toString());
              } else {
                value.put(fieldName, "Non-displayable object");
              }
            } else {
              value.put(fieldName, null);
            }
          }
          values.add(value);
        }

        // Save the recipes being executed.
        ws.updateWorkspaceRequest(namespacedId, directiveRequest);

        return new DirectiveExecutionResponse(values, headers, types, directiveRequest.getRecipe().getDirectives());
      } catch (JsonParseException | DirectiveParseException e) {
        throw new BadRequestException(e.getMessage(), e);
      }
    });
  }

  /**
   * Automatically adds a load-directives pragma to the list of directives.
   */
  private String addLoadablePragmaDirectives(String namespace, Request request) {
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
      while (iterator.hasNext()) {
        TokenGroup next = iterator.next();
        if (next == null || next.size() < 1) {
          continue;
        }
        Token token = next.get(0);
        if (token.type() == TokenType.DIRECTIVE_NAME) {
          String directive = (String) token.value();
          try {
            DirectiveInfo.Scope scope = composite.get(namespace, directive).scope();
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

  @POST
  @Path("workspaces/{id}/summary")
  public void summary(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("id") String id) {
    summary(request, responder, getContext().getNamespace(), id);
  }

  /**
   * Summarizes the workspace by running directives.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace data to be summarized.
   */
  @POST
  @Path("contexts/{context}/workspaces/{id}/summary")
  public void summary(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      try {
        RequestExtractor handler = new RequestExtractor(request);
        Request directiveRequest = handler.getContent("UTF-8", Request.class);
        if (directiveRequest == null) {
          throw new BadRequestException("Request body is empty.");
        }
        int limit = directiveRequest.getSampling().getLimit();
        List<Row> rows = executeDirectives(new NamespacedId(namespace, id), directiveRequest, records -> {
          if (records == null) {
            return Collections.emptyList();
          }
          int min = Math.min(records.size(), limit);
          return records.subList(0, min);
        });

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

        Map<String, ColumnValidationResult> columnValidationResults = new HashMap<>();
        for (String name : uniqueColumns) {
          try {
            validator.validate(name);
            columnValidationResults.put(name, new ColumnValidationResult(null));
          } catch (ValidatorException e) {
            columnValidationResults.put(name, new ColumnValidationResult(e.getMessage()));
          }
        }

        // Generate General and Type related Statistics for each column.
        Statistics statsGenerator = new BasicStatistics();
        Row summary = statsGenerator.aggregate(rows);

        Row stats = (Row) summary.getValue("stats");
        Row types = (Row) summary.getValue("types");

        List<Pair<String, Object>> fields = stats.getFields();
        Map<String, ColumnStatistics> statistics = new HashMap<>();
        for (Pair<String, Object> field : fields) {
          List<Pair<String, Double>> values = (List<Pair<String, Double>>) field.getSecond();
          Map<String, Float> generalStats = new HashMap<>();
          for (Pair<String, Double> value : values) {
            generalStats.put(value.getFirst(), value.getSecond().floatValue() * 100);
          }
          ColumnStatistics columnStatistics = new ColumnStatistics(generalStats, null);
          statistics.put(field.getFirst(), columnStatistics);
        }

        fields = types.getFields();
        for (Pair<String, Object> field : fields) {
          List<Pair<String, Double>> values = (List<Pair<String, Double>>) field.getSecond();
          Map<String, Float> typeStats = new HashMap<>();
          for (Pair<String, Double> value : values) {
            typeStats.put(value.getFirst(), value.getSecond().floatValue() * 100);
          }
          ColumnStatistics existingStats = statistics.get(field.getFirst());
          Map<String, Float> generalStats = existingStats == null ? null : existingStats.getGeneral();
          statistics.put(field.getFirst(), new ColumnStatistics(generalStats, typeStats));
        }

        WorkspaceValidationResult validationResult = new WorkspaceValidationResult(columnValidationResults, statistics);
        return new WorkspaceSummaryResponse(validationResult);
      } catch (JsonParseException | DirectiveParseException e) {
        throw new BadRequestException(e.getMessage(), e);
      }
    });
  }

  @POST
  @Path("workspaces/{id}/schema")
  public void schema(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("id") String id) {
    schema(request, responder, getContext().getNamespace(), id);
  }

  @POST
  @Path("contexts/{context}/workspaces/{id}/schema")
  public void schema(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, () -> {
      RequestExtractor handler = new RequestExtractor(request);
      Request user = handler.getContent("UTF-8", Request.class);
      if (user == null) {
        throw new BadRequestException("Request body is empty.");
      }
      int limit = user.getSampling().getLimit();
      List<Row> rows = executeDirectives(new NamespacedId(namespace, id), user, records -> {
        if (records == null) {
          return Collections.emptyList();
        }
        int min = Math.min(records.size(), limit);
        return records.subList(0, min);
      });

      // generate a schema based upon the first record
      Json2Schema json2Schema = new Json2Schema();
      Schema schema = json2Schema.toSchema("record", createUberRecord(rows));
      if (schema.getType() != Schema.Type.RECORD) {
        schema = Schema.recordOf("array", Schema.Field.of("value", schema));
      }

      String schemaJson = GSON.toJson(schema);
      // the current contract with the UI is not to pass the
      // entire schema string, but just the fields.
      return new JsonParser().parse(schemaJson)
        .getAsJsonObject()
        .get("fields").getAsJsonArray();
    });
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
    respond(request, responder, () -> {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      Properties props = new Properties();
      try (InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
        props.load(resourceStream);
      } catch (IOException e) {
        throw new IOException("There was problem reading the capability matrix. " +
          "Please check the environment to ensure you have right verions of jar." + e.getMessage(), e);
      }

      // this is a weird API, it should be object with 'key' and 'value' fields instead of the key being the key value.
      List<Map<String, String>> values = new ArrayList<>();
      for (String key : props.stringPropertyNames()) {
        values.add(Collections.singletonMap(key, props.getProperty(key)));
      }
      return new ServiceResponse<>(values);
    });
  }


  @GET
  @Path("usage")
  public void usage(HttpServiceRequest request, HttpServiceResponder responder) {
    usage(request, responder, getContext().getNamespace());
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
  @Path("contexts/{context}/usage")
  public void usage(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      composite.reload(namespace);
      DirectiveConfig config = ws.getConfig();
      Map<String, List<String>> aliases = config.getReverseAlias();
      List<DirectiveUsage> values = new ArrayList<>();

      for (DirectiveInfo directive : composite.list(namespace)) {
        DirectiveUsage directiveUsage = new DirectiveUsage(directive.name(), directive.usage(), directive.description(),
                                                           config.isExcluded(directive.name()), false,
                                                           directive.scope().name(), directive.definition(),
                                                           directive.categories());
        values.add(directiveUsage);

        // For this directive we find all aliases and add them to the
        // description.
        if (aliases.containsKey(directive.name())) {
          List<String> list = aliases.get(directive.name());
          for (String alias : list) {
            directiveUsage = new DirectiveUsage(alias, directive.usage(), directive.description(),
                                                config.isExcluded(directive.name()), true,
                                                directive.scope().name(), directive.definition(),
                                                directive.categories());
            values.add(directiveUsage);
          }
        }
      }

      return new ServiceResponse<>(values);
    });
  }

  @GET
  @Path("artifacts")
  public void artifacts(HttpServiceRequest request, HttpServiceResponder responder) {
    artifacts(request, responder, getContext().getNamespace());
  }

  /**
   * This HTTP endpoint is used to retrieve artifacts that include plugins
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("contexts/{context}/artifacts")
  public void artifacts(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      List<DirectiveArtifact> values = new ArrayList<>();
      List<ArtifactInfo> artifacts = getContext().listArtifacts(namespace);
      for (ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        for (PluginClass plugin : plugins) {
          if (Directive.TYPE.equalsIgnoreCase(plugin.getType())) {
            values.add(new DirectiveArtifact(artifact));
            break;
          }
        }
      }
      return new ServiceResponse<>(values);
    });
  }

  @GET
  @Path("directives")
  public void directives(HttpServiceRequest request, HttpServiceResponder responder) {
    directives(request, responder, getContext().getNamespace());
  }

  /**
   * This HTTP endpoint is used to retrieve plugins that are
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   *
   * TODO: (CDAP-14694) Doesn't look like this should exist. Why wasn't the CDAP endpoint used?
   */
  @GET
  @Path("contexts/{context}/directives")
  public void directives(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      List<DirectiveDescriptor> values = new ArrayList<>();
      List<ArtifactInfo> artifacts = getContext().listArtifacts(namespace);
      for (ArtifactInfo artifact : artifacts) {
        Set<PluginClass> plugins = artifact.getClasses().getPlugins();
        DirectiveArtifact directiveArtifact =
          new DirectiveArtifact(artifact.getName(), artifact.getVersion(), artifact.getScope().name());
        for (PluginClass plugin : plugins) {
          if (Directive.TYPE.equalsIgnoreCase(plugin.getType())) {
            values.add(new DirectiveDescriptor(plugin, directiveArtifact));
          }
        }
      }
      return new ServiceResponse<>(values);
    });
  }

  @GET
  @Path("directives/reload")
  public void directivesReload(HttpServiceRequest request, HttpServiceResponder responder) {
    directivesReload(request, responder, getContext().getNamespace());
  }

  /**
   * This HTTP endpoint is used to reload the plugins that are
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("contexts/{context}/directives/reload")
  public void directivesReload(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace) {
    respond(request, responder, namespace, () -> {
      composite.reload(namespace);
      return new ServiceResponse<Void>("Successfully reloaded all user defined directives.");
    });
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
    respond(request, responder, () -> new ServiceResponse<>(Charset.availableCharsets().keySet()));
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
    respond(request, responder, () -> {
      // Read the request body
      RequestExtractor handler = new RequestExtractor(request);
      DirectiveConfig config = handler.getContent("UTF-8", DirectiveConfig.class);
      if (config == null) {
        throw new BadRequestException("Config is empty. Please check if the request is sent as HTTP POST body.");
      }
      ws.updateConfig(config);
      return new ServiceResponse<Void>("Successfully updated configuration.");
    });
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
    respond(request, responder, () -> new ServiceResponse<Void>(ws.getConfigString()));
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
   * @param workspace the workspace to get records from
   * @return list of records.
   */
  private List<Row> fromWorkspace(Workspace workspace) throws IOException, ClassNotFoundException {
    DataType type = workspace.getType();
    List<Row> rows = new ArrayList<>();

    switch(type) {
      case TEXT: {
        String data = Bytes.toString(workspace.getData());
        if (data != null) {
          rows.add(new Row("body", data));
        }
        break;
      }

      case BINARY: {
        byte[] data = workspace.getData();
        if (data != null) {
          rows.add(new Row("body", data));
        }
        break;
      }

      case RECORDS: {
        if (workspace.getData() != null) {
          try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(workspace.getData()))) {
            rows = (List<Row>) ois.readObject();
          }
        }
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
  private List<Row> executeDirectives(NamespacedId id, @Nullable Request user,
                                      Function<List<Row>, List<Row>> sample) throws Exception {
    if (user == null) {
      throw new BadRequestException("Request is empty. Please check if the request is sent as HTTP POST body.");
    }

    TransientStore store = new DefaultTransientStore();
    Workspace workspace = ws.getWorkspace(id);
    // Extract rows from the workspace.
    List<Row> rows = fromWorkspace(workspace);
    // Execute the pipeline.
    ExecutorContext context = new ServicePipelineContext(id.getNamespace(), ExecutorContext.Environment.SERVICE,
                                                         getContext(),
                                                         store);
    RecipePipelineExecutor executor = new RecipePipelineExecutor();
    if (user.getRecipe().getDirectives().size() > 0) {
      GrammarMigrator migrator = new MigrateToV2(user.getRecipe().getDirectives());
      String migrate = migrator.migrate();
      RecipeParser recipe = new GrammarBasedParser(id.getNamespace(), migrate, composite);
      recipe.initialize(new ConfigDirectiveContext(ws.getConfigString()));
      executor.initialize(recipe, context);
      rows = executor.execute(sample.apply(rows));
      executor.destroy();
    }
    return rows;
  }
}
