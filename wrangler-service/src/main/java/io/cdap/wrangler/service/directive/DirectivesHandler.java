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

package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.SamplingMethod;
import io.cdap.wrangler.ServiceUtils;
import io.cdap.wrangler.api.CompileException;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRecordBase;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.RecipeSymbol;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.dataset.workspace.ConfigStore;
import io.cdap.wrangler.dataset.workspace.DataType;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.dataset.workspace.WorkspaceMeta;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.GrammarBasedParser;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.ConflictException;
import io.cdap.wrangler.proto.ErrorRecordsException;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.Request;
import io.cdap.wrangler.proto.ServiceResponse;
import io.cdap.wrangler.proto.WorkspaceIdentifier;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.workspace.ColumnStatistics;
import io.cdap.wrangler.proto.workspace.ColumnValidationResult;
import io.cdap.wrangler.proto.workspace.DataModelInfo;
import io.cdap.wrangler.proto.workspace.DirectiveArtifact;
import io.cdap.wrangler.proto.workspace.DirectiveDescriptor;
import io.cdap.wrangler.proto.workspace.DirectiveExecutionResponse;
import io.cdap.wrangler.proto.workspace.DirectiveUsage;
import io.cdap.wrangler.proto.workspace.ModelInfo;
import io.cdap.wrangler.proto.workspace.WorkspaceInfo;
import io.cdap.wrangler.proto.workspace.WorkspaceSummaryResponse;
import io.cdap.wrangler.proto.workspace.WorkspaceValidationResult;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.registry.UserDirectiveRegistry;
import io.cdap.wrangler.service.common.AbstractWranglerHandler;
import io.cdap.wrangler.service.schema.DataModelGlossary;
import io.cdap.wrangler.statistics.BasicStatistics;
import io.cdap.wrangler.statistics.Statistics;
import io.cdap.wrangler.utils.Json2Schema;
import io.cdap.wrangler.utils.ObjectSerDe;
import io.cdap.wrangler.validator.ColumnNameValidator;
import io.cdap.wrangler.validator.Validator;
import io.cdap.wrangler.validator.ValidatorException;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.HttpURLConnection;
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
import java.util.stream.Collectors;
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
  private static final String DATA_MODEL_PROPERTY = "dataModel";
  private static final String DATA_MODEL_REVISION_PROPERTY = "dataModelRevision";
  private static final String DATA_MODEL_MODEL_PROPERTY = "dataModelModel";

  private DirectiveRegistry composite;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
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

  @GET
  @Path("health")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void healthCheck(HttpServiceRequest request, HttpServiceResponder responder) {
    responder.sendStatus(HttpURLConnection.HTTP_OK);
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void create(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("context") String namespace,
                     @PathParam("id") String id, @QueryParam("name") String name,
                     @QueryParam("scope") @DefaultValue(WorkspaceDataset.DEFAULT_SCOPE) String scope) {
    respond(request, responder, namespace, ns -> {
      String workspaceName = name == null || name.isEmpty() ? id : name;

      Map<String, String> properties = new HashMap<>();
      properties.put(PropertyIds.NAME, workspaceName);
      NamespacedId workspaceId = new NamespacedId(ns, id);
      WorkspaceMeta workspaceMeta = WorkspaceMeta.builder(workspaceName)
        .setScope(scope)
        .setProperties(properties)
        .build();
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.writeWorkspaceMeta(workspaceId, workspaceMeta);
      });
      return new ServiceResponse<Void>(String.format("Successfully created workspace '%s'", id));
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void list(HttpServiceRequest request, HttpServiceResponder responder,
                   @PathParam("context") String namespace, @QueryParam("scope") @DefaultValue("default") String scope) {
    respond(request, responder, namespace, ns -> {
      List<WorkspaceIdentifier> workspaces = TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        return ws.listWorkspaces(ns, scope);
      });
      return new ServiceResponse<>(workspaces);
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void delete(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.deleteWorkspace(new NamespacedId(ns, id));
      });
      return new ServiceResponse<Void>(String.format("Successfully deleted workspace '%s'", id));
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void deleteGroup(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace, @QueryParam("group") String group) {
    respond(request, responder, namespace, ns -> {
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.deleteScope(ns, group);
      });
      return new ServiceResponse<Void>(String.format("Successfully deleted workspaces within group '%s'", group));
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      Workspace workspace = getWorkspace(new NamespacedId(ns, id));
      String name = workspace.getName();
      Request workspaceReq = workspace.getRequest();
      JsonObject req = new JsonObject();
      if (workspaceReq != null) {
        req = (JsonObject) GSON.toJsonTree(workspaceReq);
      }
      Map<String, String> properties = workspace.getProperties();
      JsonObject prop = (JsonObject) GSON.toJsonTree(properties);

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

  /**
   * Upload data to the workspace, the workspace is created automatically on fly.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   */
  @POST
  @Path("contexts/{context}/workspaces")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      String name = request.getHeader(PropertyIds.FILE_NAME);
      if (name == null) {
        throw new BadRequestException("Name must be provided in the 'file' header");
      }
      NamespacedId id = new NamespacedId(ns, ServiceUtils.generateMD5(name));

      return TransactionRunners.run(getContext(), context -> {
        // if workspace doesn't exist, then we create the workspace before
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        // adding data to the workspace.
        if (!ws.hasWorkspace(id)) {
          ws.writeWorkspaceMeta(id, WorkspaceMeta.builder(name).build());
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
          throw new BadRequestException("Body not present, please post the file containing the "
                                          + "records to be wrangled.");
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
        properties.put(PropertyIds.NAME, name);
        properties.put(PropertyIds.DELIMITER, delimiter);
        properties.put(PropertyIds.CHARSET, charset);
        properties.put(PropertyIds.CONTENT_TYPE, contentType);
        properties.put(PropertyIds.CONNECTION_TYPE, ConnectionType.UPLOAD.getType());
        ws.updateWorkspaceProperties(id, properties);

        WorkspaceInfo workspaceInfo = new WorkspaceInfo(id.getId(), name, delimiter, charset, contentType,
                                                        ConnectionType.UPLOAD.getType(),
                                                        SamplingMethod.NONE.getMethod());
        return new ServiceResponse<>(workspaceInfo);
      });
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void uploadData(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      RequestExtractor handler = new RequestExtractor(request);

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
      NamespacedId namespaceId = new NamespacedId(ns, id);

      return TransactionRunners.run(getContext(), context -> {
        // For back-ward compatibility, we check if there is delimiter specified
        // using 'recorddelimiter' or 'delimiter'
        String delimiter = handler.getHeader(RECORD_DELIMITER_HEADER, "\\u001A");
        delimiter = handler.getHeader(DELIMITER_HEADER, delimiter);

        WorkspaceDataset ws = WorkspaceDataset.get(context);
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
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void execute(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      composite.reload(namespace);
      try {
        RequestExtractor handler = new RequestExtractor(request);
        Request directiveRequest = handler.getContent("UTF-8", Request.class);
        if (directiveRequest == null) {
          throw new BadRequestException("Request body is empty.");
        }
        directiveRequest.getRecipe().setPragma(addLoadablePragmaDirectives(namespace, directiveRequest));

        int limit = directiveRequest.getSampling().getLimit();
        NamespacedId namespacedId = new NamespacedId(ns, id);
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
        TransactionRunners.run(getContext(), context -> {
          WorkspaceDataset ws = WorkspaceDataset.get(context);
          ws.updateWorkspaceRequest(namespacedId, directiveRequest);
        });

        return new DirectiveExecutionResponse(values, headers, types, directiveRequest.getRecipe().getDirectives());
      } catch (JsonParseException e) {
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
    } catch (CompileException | DirectiveParseException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
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
  @Path("contexts/{context}/workspaces/{id}/summary")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void summary(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      try {
        composite.reload(namespace);
        RequestExtractor handler = new RequestExtractor(request);
        Request directiveRequest = handler.getContent("UTF-8", Request.class);
        if (directiveRequest == null) {
          throw new BadRequestException("Request body is empty.");
        }
        int limit = directiveRequest.getSampling().getLimit();
        List<Row> rows = executeDirectives(new NamespacedId(ns, id), directiveRequest, records -> {
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
  @Path("contexts/{context}/workspaces/{id}/schema")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void schema(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      composite.reload(namespace);
      RequestExtractor handler = new RequestExtractor(request);
      Request user = handler.getContent("UTF-8", Request.class);
      if (user == null) {
        throw new BadRequestException("Request body is empty.");
      }
      int limit = user.getSampling().getLimit();
      List<Row> rows = executeDirectives(new NamespacedId(ns, id), user, records -> {
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
   * Adds a data model to workspace. There is a one-to-one mapping between workspaces and models.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace to associate a data model.
   */
  @POST
  @Path("contexts/{context}/workspaces/{id}/datamodels")
  public void addDataModel(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      NamespacedId namespacedId = new NamespacedId(ns, id);
      Workspace workspace = getWorkspace(namespacedId);
      RequestExtractor handler = new RequestExtractor(request);
      DataModelInfo dataModelInfo = handler.getContent("UTF-8", DataModelInfo.class);
      if (dataModelInfo == null || dataModelInfo.getId() == null || dataModelInfo.getRevision() == null) {
        throw new BadRequestException("request body is missing required id and revision fields.");
      }
      org.apache.avro.Schema schema = DataModelGlossary.getGlossary()
        .get(dataModelInfo.getId(), dataModelInfo.getRevision());
      if (schema == null) {
        throw new BadRequestException(String.format("Unable to find data model %s revision %d", dataModelInfo.getId(),
                                                    dataModelInfo.getRevision()));
      }

      Map<String, String> properties = new HashMap<>(workspace.getProperties());
      if (properties.get(DATA_MODEL_PROPERTY) != null) {
        throw new ConflictException("data model property already set");
      }

      properties.put(DATA_MODEL_PROPERTY, dataModelInfo.getId());
      properties.put(DATA_MODEL_REVISION_PROPERTY, Long.toString(dataModelInfo.getRevision()));

      // Save the properties that were added
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.updateWorkspaceProperties(namespacedId, properties);
      });

      return new ServiceResponse<>(dataModelInfo);
    });
  }

  /**
   * Removes a data model from a workspace.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace to disassociate a data model from.
   */
  @DELETE
  @Path("contexts/{context}/workspaces/{id}/datamodels")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void removeDataModel(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      NamespacedId namespacedId = new NamespacedId(ns, id);
      Workspace workspace = getWorkspace(namespacedId);
      Map<String, String> properties = new HashMap<>(workspace.getProperties());
      properties.remove(DATA_MODEL_PROPERTY);
      properties.remove(DATA_MODEL_REVISION_PROPERTY);

      // Save the properties that were added
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.updateWorkspaceProperties(namespacedId, properties);
      });
      return new ServiceResponse<Void>("successfully removed the data model from the workspace");
    });
  }

  /**
   * Adds a model to the workspace. There is a one-to-one mapping between workspace and model. In addition, the model
   * must be a member of the data model associated with the workspace. If not, an error will be raised.
   *
   * @param request Handler for incoming request.
   * @param responder Responder for data going out.
   * @param id Workspace to associated a model with.
   */
  @POST
  @Path("contexts/{context}/workspaces/{id}/models")
  public void addModels(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace, @PathParam("id") String id) {
    respond(request, responder, namespace, ns -> {
      NamespacedId namespacedId = new NamespacedId(ns, id);
      Workspace workspace = getWorkspace(namespacedId);
      RequestExtractor handler = new RequestExtractor(request);
      ModelInfo model = handler.getContent("UTF-8", ModelInfo.class);
      if (model.getId() == null) {
        throw new BadRequestException("request body is empty.");
      }

      Map<String, String> properties = new HashMap<>(workspace.getProperties());
      Long revision = null;
      try {
        revision = Long.parseLong(properties.get(DATA_MODEL_REVISION_PROPERTY));
      } catch (NumberFormatException e) {
        revision = DataModelInfo.INVALID_REVISION;
      }
      DataModelInfo dataModelInfo = new DataModelInfo(properties.get(DATA_MODEL_PROPERTY), revision);
      if (dataModelInfo.getId() == null || dataModelInfo.getRevision().equals(DataModelInfo.INVALID_REVISION)) {
        throw new BadRequestException("data model or data model revision properties has not been set.");
      }
      
      if (properties.get(DATA_MODEL_MODEL_PROPERTY) != null) {
        throw new ConflictException("model property already set.");
      }

      org.apache.avro.Schema schema = DataModelGlossary.getGlossary()
        .get(dataModelInfo.getId(), dataModelInfo.getRevision());
      List<org.apache.avro.Schema.Field> fieldMatch = schema.getFields().stream()
        .filter(field -> field.name().equals(model.getId()))
        .collect(Collectors.toList());
      if (fieldMatch.isEmpty()) {
        throw new NotFoundException(
          String.format("Unable to find model %s in data model %s revision %d.", model.getId(), dataModelInfo.getId(),
                        dataModelInfo.getRevision()));
      }

      properties.put(DATA_MODEL_MODEL_PROPERTY, model.getId());

      // Save the properties that were added
      TransactionRunners.run(getContext(), context -> {
        WorkspaceDataset ws = WorkspaceDataset.get(context);
        ws.updateWorkspaceProperties(namespacedId, properties);
      });

      return new ServiceResponse<>(dataModelInfo);
    });
  }

  /**
   *
   */
  @DELETE
  @Path("contexts/{context}/workspaces/{id}/models/{modelid}")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void removeModels(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace, @PathParam("id") String id,
                           @PathParam("modelid") String modelId) {
    respond(request, responder, namespace, ns -> {
      NamespacedId namespacedId = new NamespacedId(ns, id);
      Workspace workspace = getWorkspace(namespacedId);
      Map<String, String> properties = new HashMap<>(workspace.getProperties());
      if (properties.containsKey(DATA_MODEL_MODEL_PROPERTY)) {
        if (!modelId.equals(properties.get(DATA_MODEL_MODEL_PROPERTY))) {
          throw new NotFoundException(String.format("model %s is not a property of the workspace.", modelId));
        }
        properties.remove(DATA_MODEL_MODEL_PROPERTY);

        // Save the properties that were added
        TransactionRunners.run(getContext(), context -> {
          WorkspaceDataset ws = WorkspaceDataset.get(context);
          ws.updateWorkspaceProperties(namespacedId, properties);
        });
      }
      return new ServiceResponse<Void>("successfully removed the data model from the workspace");
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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

      Map<String, String> value = new HashMap<>();
      for (String key : props.stringPropertyNames()) {
        value.put(key, props.getProperty(key));
      }
      return new ServiceResponse<>(value);
    });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void usage(HttpServiceRequest request, HttpServiceResponder responder,
                    @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
      // CDAP-15397 - reload must be called before it can be safely used
      composite.reload(namespace);
      DirectiveConfig config = TransactionRunners.run(getContext(), context -> {
        ConfigStore store = ConfigStore.get(context);
        return store.getConfig();
      });
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

  /**
   * This HTTP endpoint is used to retrieve artifacts that include plugins
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("contexts/{context}/artifacts")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void artifacts(HttpServiceRequest request, HttpServiceResponder responder,
                        @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
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
  /**
   * This HTTP endpoint is used to retrieve plugins that are
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   *
   * TODO: (CDAP-14694) Doesn't look like this should exist. Why wasn't the CDAP endpoint used?
   */
  @GET
  @Path("contexts/{context}/directives")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void directives(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
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

  /**
   * This HTTP endpoint is used to reload the plugins that are
   * of type <code>Directive.Type</code> (directive). Artifact will be reported
   * if it atleast has one plugin that is of type "directive".
   */
  @GET
  @Path("contexts/{context}/directives/reload")
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void directivesReload(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("context") String namespace) {
    respond(request, responder, namespace, ns -> {
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void uploadConfig(HttpServiceRequest request, HttpServiceResponder responder) {
    respond(request, responder, () -> {
      // Read the request body
      RequestExtractor handler = new RequestExtractor(request);
      DirectiveConfig config = handler.getContent("UTF-8", DirectiveConfig.class);
      if (config == null) {
        throw new BadRequestException("Config is empty. Please check if the request is sent as HTTP POST body.");
      }
      TransactionRunners.run(getContext(), context -> {
        ConfigStore configStore = ConfigStore.get(context);
        configStore.updateConfig(config);
      });
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
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  public void getConfig(HttpServiceRequest request, HttpServiceResponder responder) {
    respond(request, responder, () -> TransactionRunners.run(getContext(), context -> {
      ConfigStore configStore = ConfigStore.get(context);
      return new ServiceResponse<>(configStore.getConfig());
    }));
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
                                      Function<List<Row>, List<Row>> sample) {
    if (user == null) {
      throw new BadRequestException("Request is empty. Please check if the request is sent as HTTP POST body.");
    }

    TransientStore store = new DefaultTransientStore();
    return TransactionRunners.run(getContext(), ctx -> {
      WorkspaceDataset ws = WorkspaceDataset.get(ctx);

      Workspace workspace = ws.getWorkspace(id);
      // Extract rows from the workspace.
      List<Row> rows = fromWorkspace(workspace);
      // Execute the pipeline.
      ExecutorContext context = new ServicePipelineContext(id.getNamespace().getName(),
                                                           ExecutorContext.Environment.SERVICE, getContext(), store);
      RecipePipelineExecutor executor = new RecipePipelineExecutor();
      if (user.getRecipe().getDirectives().size() > 0) {
        ConfigStore configStore = ConfigStore.get(ctx);
        GrammarMigrator migrator = new MigrateToV2(user.getRecipe().getDirectives());
        String migrate = migrator.migrate();
        RecipeParser recipe = new GrammarBasedParser(id.getNamespace().getName(), migrate, composite);
        recipe.initialize(new ConfigDirectiveContext(configStore.getConfig()));
        try {
          executor.initialize(recipe, context);
          rows = executor.execute(sample.apply(rows));
        } catch (RecipeException e) {
          throw new BadRequestException(e.getMessage(), e);
        }

        List<ErrorRecordBase> errors = executor.errors()
          .stream()
          .filter(ErrorRecordBase::isShownInWrangler)
          .collect(Collectors.toList());
        if (errors.size() > 0) {
          throw new ErrorRecordsException(errors);
        }

        executor.destroy();
      }
      return rows;
    });
  }
}
