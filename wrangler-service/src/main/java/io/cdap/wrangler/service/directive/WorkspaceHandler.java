/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.service.directive;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.annotation.TransactionPolicy;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metrics.Metrics;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.common.Constants;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.GrammarMigrator;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RemoteDirectiveResponse;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.parser.ConfigDirectiveContext;
import io.cdap.wrangler.parser.DirectiveClass;
import io.cdap.wrangler.parser.GrammarWalker;
import io.cdap.wrangler.parser.MigrateToV2;
import io.cdap.wrangler.parser.RecipeCompiler;
import io.cdap.wrangler.proto.BadRequestException;
import io.cdap.wrangler.proto.recipe.v2.Recipe;
import io.cdap.wrangler.proto.recipe.v2.RecipeId;
import io.cdap.wrangler.proto.workspace.v2.Artifact;
import io.cdap.wrangler.proto.workspace.v2.DirectiveExecutionRequest;
import io.cdap.wrangler.proto.workspace.v2.DirectiveExecutionResponse;
import io.cdap.wrangler.proto.workspace.v2.DirectiveUsage;
import io.cdap.wrangler.proto.workspace.v2.Plugin;
import io.cdap.wrangler.proto.workspace.v2.SampleSpec;
import io.cdap.wrangler.proto.workspace.v2.ServiceResponse;
import io.cdap.wrangler.proto.workspace.v2.StageSpec;
import io.cdap.wrangler.proto.workspace.v2.Workspace;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceCreationRequest;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceDetail;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceId;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceSpec;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceUpdateRequest;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import io.cdap.wrangler.schema.TransientStoreKeys;
import io.cdap.wrangler.store.recipe.RecipeStore;
import io.cdap.wrangler.store.workspace.WorkspaceStore;
import io.cdap.wrangler.utils.KryoSerializer;
import io.cdap.wrangler.utils.ObjectSerDe;
import io.cdap.wrangler.utils.RowHelper;
import io.cdap.wrangler.utils.SchemaConverter;
import io.cdap.wrangler.utils.StructuredToRowTransformer;
import org.apache.commons.lang3.StringEscapeUtils;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import static io.cdap.wrangler.schema.TransientStoreKeys.INPUT_SCHEMA;
import static io.cdap.wrangler.schema.TransientStoreKeys.OUTPUT_SCHEMA;

/**
 * V2 endpoints for workspace
 */
public class WorkspaceHandler extends AbstractDirectiveHandler {

  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();
  private static final Pattern PRAGMA_PATTERN = Pattern.compile("^\\s*#pragma\\s+load-directives\\s+");
  private static final String UPLOAD_COUNT = "upload.file.count";
  private static final String CONNECTION_TYPE = "upload";

  private WorkspaceStore wsStore;
  private RecipeStore recipeStore;
  private ConnectionDiscoverer discoverer;

  // Injected by CDAP
  @SuppressWarnings("unused")
  private Metrics metrics;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    wsStore = new WorkspaceStore(context);
    recipeStore = new RecipeStore(context);
    discoverer = new ConnectionDiscoverer(context);
  }

  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces")
  public void createWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Creating workspace in system namespace is currently not supported");
      }

      WorkspaceCreationRequest creationRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), WorkspaceCreationRequest.class);

      if (creationRequest.getConnection() == null) {
        throw new BadRequestException("Connection name has to be provided to create a workspace");
      }

      SampleRequest sampleRequest = creationRequest.getSampleRequest();
      if (sampleRequest == null) {
        throw new BadRequestException("Sample request has to be provided to create a workspace");
      }

      SampleResponse sampleResponse = discoverer.retrieveSample(namespace, creationRequest.getConnection(),
                                                                sampleRequest);
      List<Row> rows = getSample(sampleResponse);

      ConnectorDetail detail = sampleResponse.getDetail();
      SampleSpec spec = new SampleSpec(
        creationRequest.getConnection(), creationRequest.getConnectionType(), sampleRequest.getPath(),
        detail.getRelatedPlugins().stream().map(plugin -> {
          ArtifactSelectorConfig artifact = plugin.getArtifact();
          Plugin pluginSpec = new Plugin(
            plugin.getName(), plugin.getType(), plugin.getProperties(),
            new Artifact(artifact.getName(), artifact.getVersion(), artifact.getScope()));
          return new StageSpec(plugin.getSchema(), pluginSpec);
        }).collect(Collectors.toSet()), detail.getSupportedSampleTypes(), sampleRequest);

      WorkspaceId wsId = new WorkspaceId(ns);
      long now = System.currentTimeMillis();
      Workspace workspace = Workspace.builder(generateWorkspaceName(wsId, creationRequest.getSampleRequest().getPath()),
                                              wsId.getWorkspaceId())
                              .setCreatedTimeMillis(now).setUpdatedTimeMillis(now).setSampleSpec(spec).build();
      wsStore.saveWorkspace(wsId, new WorkspaceDetail(workspace, rows));
      responder.sendJson(wsId.getWorkspaceId());
    });
  }

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces")
  public void listWorkspaces(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Listing workspaces in system namespace is currently not supported");
      }
      responder.sendString(GSON.toJson(new ServiceResponse<>(wsStore.listWorkspaces(ns))));
    });
  }

  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void getWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Getting workspace in system namespace is currently not supported");
      }
      responder.sendString(GSON.toJson(wsStore.getWorkspace(new WorkspaceId(ns, workspaceId))));
    });
  }

  /**
   * Update the workspace's directives and initiatives
   */
  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void updateWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace,
                              @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Updating workspace in system namespace is currently not supported");
      }

      WorkspaceUpdateRequest updateRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), WorkspaceUpdateRequest.class);

      WorkspaceId wsId = new WorkspaceId(ns, workspaceId);
      Workspace newWorkspace = Workspace.builder(wsStore.getWorkspace(wsId))
                                 .setDirectives(updateRequest.getDirectives())
                                 .setInsights(updateRequest.getInsights())
                                 .setUpdatedTimeMillis(System.currentTimeMillis()).build();
      wsStore.updateWorkspace(wsId, newWorkspace);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Resample the workspace using a new sample request. Keeps all previously-applied directives.
   */
  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}/resample")
  public void resampleWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                                @PathParam("context") String namespace,
                                @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Resampling workspace in system namespace is currently not supported");
      }

      WorkspaceId wsId = new WorkspaceId(ns, workspaceId);
      Workspace currentWorkspace = wsStore.getWorkspace(wsId);

      String connectionName = currentWorkspace.getSampleSpec() == null ? null :
        currentWorkspace.getSampleSpec().getConnectionName();
      if (connectionName == null) {
        throw new BadRequestException("Connection name has to exist to resample a workspace");
      }

      String sampleRequestString = StandardCharsets.UTF_8.decode(request.getContent()).toString();
      // sampleRequestString looks like {"sampleRequest": {...}}, so to parse it into SampleRequest.class
      // we first have to extract the inner object
      JsonElement sampleRequestJson =
        new JsonParser().parse(sampleRequestString).getAsJsonObject().get("sampleRequest");
      SampleRequest sampleRequest = GSON.fromJson(sampleRequestJson, SampleRequest.class);
      if (sampleRequest == null) {
        throw new BadRequestException("Sample request has to be provided to resample a workspace");
      }

      SampleResponse sampleResponse = discoverer.retrieveSample(namespace, connectionName,
                                                                sampleRequest);
      List<Row> rows = getSample(sampleResponse);

      SampleSpec oldSpec = currentWorkspace.getSampleSpec();
      SampleSpec newSpec = new SampleSpec(oldSpec.getConnectionName(), oldSpec.getConnectionType(), oldSpec.getPath(),
              oldSpec.getRelatedPlugins(), oldSpec.getSupportedSampleTypes(), sampleRequest);

      Workspace newWorkspace = Workspace.builder(currentWorkspace)
        .setUpdatedTimeMillis(System.currentTimeMillis())
        .setSampleSpec(newSpec).build();
      wsStore.saveWorkspace(wsId, new WorkspaceDetail(newWorkspace, rows));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @DELETE
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void deleteWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace,
                              @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Deleting workspace in system namespace is currently not supported");
      }
      wsStore.deleteWorkspace(new WorkspaceId(ns, workspaceId));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Upload data to the workspace, the workspace is created automatically on fly.
   */
  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/upload")
  public void upload(HttpServiceRequest request, HttpServiceResponder responder,
                     @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Uploading data in system namespace is currently not supported");
      }

      String name = request.getHeader(PropertyIds.FILE_NAME);
      if (name == null) {
        throw new BadRequestException("Name must be provided in the 'file' header");
      }

      RequestExtractor handler = new RequestExtractor(request);

      // For back-ward compatibility, we check if there is delimiter specified
      // using 'recorddelimiter' or 'delimiter'
      String delimiter = handler.getHeader(RECORD_DELIMITER_HEADER, "\\u001A");
      delimiter = handler.getHeader(DELIMITER_HEADER, delimiter);
      String content = handler.getContent(StandardCharsets.UTF_8);
      if (content == null) {
        throw new BadRequestException(
          "Body not present, please post the file containing the records to create a workspace.");
      }

      delimiter = StringEscapeUtils.unescapeJava(delimiter);
      List<Row> sample = new ArrayList<>();
      for (String line : content.split(delimiter)) {
        sample.add(new Row(COLUMN_NAME, line));
      }

      WorkspaceId id = new WorkspaceId(ns);
      long now = System.currentTimeMillis();
      Workspace workspace = Workspace.builder(name, id.getWorkspaceId())
                              .setCreatedTimeMillis(now).setUpdatedTimeMillis(now).build();
      wsStore.saveWorkspace(id, new WorkspaceDetail(workspace, sample));
      Metrics child = metrics.child(ImmutableMap.of(Constants.Metrics.Tag.APP_ENTITY_TYPE,
                                                    Constants.CONNECTION_SERVICE_NAME,
                                                    Constants.Metrics.Tag.APP_ENTITY_TYPE_NAME,
                                                    CONNECTION_TYPE));
      child.count(UPLOAD_COUNT, 1);
      responder.sendJson(id.getWorkspaceId());
    });
  }

  /**
   * Executes the directives on the record.
   */
  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}/execute")
  public void execute(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace,
                      @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      validateNamespace(ns, "Executing directives in system namespace is currently not supported");

      DirectiveExecutionResponse response = execute(ns, request, new WorkspaceId(ns, workspaceId),
                                                    null);
      responder.sendJson(response);
    });
  }

  /**
   * Retrieve the directives available in the namespace
   */
  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/directives")
  public void getDirectives(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      // CDAP-15397 - reload must be called before it can be safely used
      composite.reload(namespace);
      List<DirectiveUsage> directives = new ArrayList<>();
      for (DirectiveInfo directive : composite.list(namespace)) {
        DirectiveUsage directiveUsage = new DirectiveUsage(directive.name(), directive.usage(),
                                                           directive.description(), directive.scope().name(),
                                                           directive.definition(), directive.categories());
        directives.add(directiveUsage);
      }
      responder.sendJson(new ServiceResponse<>(directives));
    });
  }

  /**
   * Get the specification for the workspace
   */
  @GET
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}/specification")
  public void specification(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("context") String namespace,
                            @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Getting specification in system namespace is currently not supported");
      }
      // reload to retrieve the wrangler transform
      composite.reload(namespace);

      WorkspaceId wsId = new WorkspaceId(ns, workspaceId);
      WorkspaceDetail detail = wsStore.getWorkspaceDetail(wsId);
      List<String> directives = new ArrayList<>(detail.getWorkspace().getDirectives());
      UserDirectivesCollector userDirectivesCollector = new UserDirectivesCollector();
      List<Row> result = executeDirectives(ns.getName(), directives, detail, userDirectivesCollector);
      userDirectivesCollector.addLoadDirectivesPragma(directives);

      Schema outputSchema;
      if (schemaManagementEnabled) {
        outputSchema = TRANSIENT_STORE.get(TransientStoreKeys.OUTPUT_SCHEMA) != null ?
          TRANSIENT_STORE.get(TransientStoreKeys.OUTPUT_SCHEMA) : TRANSIENT_STORE.get(TransientStoreKeys.INPUT_SCHEMA);
      } else {
        SchemaConverter schemaConvertor = new SchemaConverter();
        outputSchema = result.isEmpty() ? null : schemaConvertor.toSchema("record", RowHelper.createMergedRow(result));
      }

      // check if the rows are empty before going to create a record schema, it will result in a 400 if empty fields
      // are passed to a record type schema
      Map<String, String> properties = ImmutableMap.of("directives", String.join("\n", directives),
                                                       "field", "*",
                                                       "precondition", "false",
                                                       "workspaceId", workspaceId);

      Set<StageSpec> srcSpecs = getSourceSpecs(detail, directives);

      ArtifactSummary wrangler = composite.getLatestWranglerArtifact();
      responder.sendString(GSON.toJson(new WorkspaceSpec(
        srcSpecs, new StageSpec(outputSchema, new Plugin("Wrangler", "transform", properties,
                             wrangler == null ? null :
                               new Artifact(wrangler.getName(), wrangler.getVersion(),
                                            wrangler.getScope().name().toLowerCase()))))));
    });
  }

  @POST
  @TransactionPolicy(value = TransactionControl.EXPLICIT)
  @Path("v2/contexts/{context}/workspaces/{id}/applyRecipe/{recipe-id}")
  public void applyRecipe(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("context") String namespace,
                          @PathParam("id") String workspaceId,
                          @PathParam("recipe-id") String recipeIdString) {
    respond(responder, namespace, ns -> {
      validateNamespace(ns, "Executing directives in system namespace is currently not supported");

      RecipeId recipeId = RecipeId.builder(ns).setRecipeId(recipeIdString).build();
      Recipe recipe = recipeStore.getRecipeById(recipeId);

      DirectiveExecutionResponse response = execute(ns, request, new WorkspaceId(ns, workspaceId),
                                                    recipe.getDirectives());
      responder.sendJson(response);
    });
  }

  private void validateNamespace(NamespaceSummary ns, String errorMessage) {
    if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
      throw new BadRequestException(errorMessage);
    }
  }

  private DirectiveExecutionResponse execute(NamespaceSummary ns, HttpServiceRequest request,
                                             WorkspaceId workspaceId,
                                             List<String> recipeDirectives) throws Exception {
    DirectiveExecutionRequest executionRequest =
      GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                    DirectiveExecutionRequest.class);

    List<String> directives = new ArrayList<>(executionRequest.getDirectives());
    if (recipeDirectives != null) {
      directives.addAll(recipeDirectives);
    }

    WorkspaceDetail detail = wsStore.getWorkspaceDetail(workspaceId);
    UserDirectivesCollector userDirectivesCollector = new UserDirectivesCollector();
    List<Row> result = executeDirectives(ns.getName(), directives, detail,
                                         userDirectivesCollector);
    DirectiveExecutionResponse response = generateExecutionResponse(result,
                                                                    executionRequest.getLimit());
    userDirectivesCollector.addLoadDirectivesPragma(directives);
    Workspace newWorkspace = Workspace.builder(detail.getWorkspace())
      .setDirectives(directives)
      .setUpdatedTimeMillis(System.currentTimeMillis()).build();
    wsStore.updateWorkspace(workspaceId, newWorkspace);
    return response;
  }

  /**
   * Get source specs, contains some hacky way on dealing with the csv parser
   */
  private Set<StageSpec> getSourceSpecs(WorkspaceDetail detail, List<String> directives) {
    SampleSpec sampleSpec = detail.getWorkspace().getSampleSpec();
    Set<StageSpec> srcSpecs = sampleSpec == null ? Collections.emptySet() : sampleSpec.getRelatedPlugins();

    // really hacky way for the parse-as-csv directive, should get removed once we have support to provide the
    // format properties when doing sampling
    boolean shouldCopyHeader =
      directives.stream()
        .map(String::trim)
        .anyMatch(directive -> directive.startsWith("parse-as-csv") && directive.endsWith("true"));
    if (shouldCopyHeader && !srcSpecs.isEmpty()) {
      srcSpecs = srcSpecs.stream().map(stageSpec -> {
        Plugin plugin = stageSpec.getPlugin();
        Map<String, String> srcProperties = new HashMap<>(plugin.getProperties());
        srcProperties.put("copyHeader", "true");
        return new StageSpec(stageSpec.getSchema(), new Plugin(plugin.getName(), plugin.getType(),
                                                               srcProperties, plugin.getArtifact()));
      }).collect(Collectors.toSet());
    }
    return srcSpecs;
  }

  /**
   * Get the workspace name, the generation rule is like:
   * 1. If the path is not null or empty, the name will be last portion of path starting from "/".
   * If "/" does not exist, the name will be the path itself.
   * 2. If path is null or empty or equal to "/", the name will be the uuid for the workspace
   */
  private String generateWorkspaceName(WorkspaceId id, @Nullable String path) {
    if (Strings.isNullOrEmpty(path)) {
      return id.getWorkspaceId();
    }

    // remove trailing "/"
    path = path.replaceAll("/+$", "");

    int last = path.lastIndexOf('/');
    // if found an "/", take the rest as name
    if (last >= 0) {
      return path.substring(last + 1);
    }
    // if not, check if path is empty or not
    return path.isEmpty() ? id.getWorkspaceId() : path;
  }

  /**
   * Executes the given list of directives on the given workspace.
   *
   * @param namespace the namespace to operate on for finding user defined directives
   * @param directives the list of directives to apply. The list provided must be a mutable list for the addition of
   *                   {@code #pragma} directives for loading UDDs.
   * @param detail the workspace to operate on
   * @param grammarVisitor visitor to call while parsing directives
   * @return the resulting rows after applying the directives
   */
  private <E extends Exception> List<Row> executeDirectives(String namespace,
                                                            List<String> directives,
                                                            WorkspaceDetail detail,
                                                            GrammarWalker.Visitor<E> grammarVisitor) throws Exception {
    // Remove all the #pragma from the existing directives. New ones will be generated.
    directives.removeIf(d -> PRAGMA_PATTERN.matcher(d).find());

    if (schemaManagementEnabled) {
      SampleSpec spec = detail.getWorkspace().getSampleSpec();
      // Workaround for uploaded files that don't have the spec set
      Schema inputSchema = spec != null ? spec.getRelatedPlugins().iterator().next().getSchema() :
        Schema.recordOf("inputSchema", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
      TRANSIENT_STORE.reset(TransientVariableScope.GLOBAL);
      TRANSIENT_STORE.set(TransientVariableScope.GLOBAL, TransientStoreKeys.INPUT_SCHEMA, inputSchema);
    }

    return getContext().isRemoteTaskEnabled() ?
      executeRemotely(namespace, directives, detail, grammarVisitor) :
      executeLocally(namespace, directives, detail, grammarVisitor);
  }

  /**
   * Executes the given list of directives on the given workspace locally in the same JVM.
   *
   * @param namespace the namespace to operate on for finding user defined directives
   * @param directives the list of directives to apply. The list provided must be a mutable list for the addition of
   *                   {@code #pragma} directives for loading UDDs.
   * @param detail the workspace to operate on
   * @param grammarVisitor visitor to call while parsing directives
   * @return the resulting rows after applying the directives
   */
  private <E extends Exception> List<Row> executeLocally(String namespace, List<String> directives,
                                   WorkspaceDetail detail, GrammarWalker.Visitor<E> grammarVisitor)
    throws DirectiveLoadException, DirectiveParseException, E, RecipeException {

    // load the udd
    composite.reload(namespace);
    return executeDirectives(namespace, directives, new ArrayList<>(detail.getSample()),
                             grammarVisitor);
  }

  /**
   * Executes the given list of directives on the given workspace remotely using the task worker framework.
   *
   * @param namespace the namespace to operate on for finding user defined directives
   * @param directives the list of directives to apply. The list provided must be a mutable list for the addition of
   *                   {@code #pragma} directives for loading UDDs.
   * @param detail the workspace to operate on
   * @param grammarVisitor visitor to call while parsing directives
   * @return the resulting rows after applying the directives
   */
  private <E extends Exception> List<Row> executeRemotely(String namespace, List<String> directives,
                                    WorkspaceDetail detail, GrammarWalker.Visitor<E> grammarVisitor) throws Exception {

    GrammarMigrator migrator = new MigrateToV2(directives);
    String recipe = migrator.migrate();
    Map<String, DirectiveClass> systemDirectives = new HashMap<>();

    // Gather system directives and call additional visitor.
    GrammarWalker walker = new GrammarWalker(new RecipeCompiler(), new ConfigDirectiveContext(DirectiveConfig.EMPTY));
    AtomicBoolean hasDirectives = new AtomicBoolean();
    walker.walk(recipe, (command, tokenGroup) -> {
      DirectiveInfo info = SystemDirectiveRegistry.INSTANCE.get(command);
      if (info != null) {
        systemDirectives.put(command, info.getDirectiveClass());
      }
      grammarVisitor.visit(command, tokenGroup);
      hasDirectives.set(true);
    });

    // If no directives to execute, just return
    if (!hasDirectives.get()) {
      return detail.getSample();
    }

    RemoteDirectiveRequest directiveRequest = new RemoteDirectiveRequest(recipe, systemDirectives,
                                                                         namespace, detail.getSampleAsBytes(),
                                                                         TRANSIENT_STORE.get(INPUT_SCHEMA));
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(RemoteExecutionTask.class.getName())
      .withParam(GSON.toJson(directiveRequest))
      .withNamespace(namespace)
      .build();
    byte[] bytes = getContext().runTask(runnableTaskRequest);
    RemoteDirectiveResponse response;
    if (Feature.WRANGLER_KRYO_SERIALIZATION.isEnabled(getContext())) {
      response = new KryoSerializer().toRemoteDirectiveResponse(bytes);
    } else {
      response = new ObjectSerDe<RemoteDirectiveResponse>().toObject(bytes);
    }
    if (response.getOutputSchema() != null) {
        TRANSIENT_STORE.set(TransientVariableScope.GLOBAL, OUTPUT_SCHEMA, response.getOutputSchema());
    }
    return response.getRows();
  }

  private List<Row> getSample(SampleResponse sampleResponse) {
    List<Row> rows = new ArrayList<>();
    if (!sampleResponse.getSample().isEmpty()) {
      for (StructuredRecord record : sampleResponse.getSample()) {
        rows.add(StructuredToRowTransformer.transform(record));
      }
    }
    return rows;
  }
}
