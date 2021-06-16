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
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.SampleResponse;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.RequestExtractor;
import io.cdap.wrangler.api.DirectiveLoadException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.proto.BadRequestException;
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
import io.cdap.wrangler.store.workspace.WorkspaceStore;
import io.cdap.wrangler.utils.ObjectSerDe;
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
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * V2 endpoints for workspace
 */
public class WorkspaceHandler extends AbstractDirectiveHandler {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  private WorkspaceStore store;
  private ConnectionDiscoverer discoverer;

  @Override
  public void initialize(SystemHttpServiceContext context) throws Exception {
    super.initialize(context);
    store = new WorkspaceStore(context);
    discoverer = new ConnectionDiscoverer(context);
  }

  @POST
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
      List<Row> rows = new ArrayList<>();
      if (!sampleResponse.getSample().isEmpty()) {
        for (StructuredRecord record : sampleResponse.getSample()) {
          rows.add(StructuredToRowTransformer.transform(record));
        }
      }

      ConnectorDetail detail = sampleResponse.getDetail();
      SampleSpec spec = new SampleSpec(
        creationRequest.getConnection(), creationRequest.getConnectionType(), sampleRequest.getPath(),
        detail.getRelatedPlugins().stream().map(plugin -> {
          ArtifactSelectorConfig artifact = plugin.getArtifact();
          Plugin pluginSpec = new Plugin(
            plugin.getName(), plugin.getType(), plugin.getProperties(),
            new Artifact(artifact.getName(), artifact.getVersion(), artifact.getScope()));
          return new StageSpec(plugin.getSchema(), pluginSpec);
        }).collect(Collectors.toSet()));

      WorkspaceId wsId = new WorkspaceId(ns);
      long now = System.currentTimeMillis();
      Workspace workspace = Workspace.builder(generateWorkspaceName(wsId, creationRequest.getSampleRequest().getPath()),
                                              wsId.getWorkspaceId())
                              .setCreatedTimeMillis(now).setUpdatedTimeMillis(now).setSampleSpec(spec).build();

      store.saveWorkspace(wsId, new WorkspaceDetail(workspace, rows));
      responder.sendJson(wsId.getWorkspaceId());
    });
  }

  @GET
  @Path("v2/contexts/{context}/workspaces")
  public void listWorkspaces(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("context") String namespace) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Listing workspaces in system namespace is currently not supported");
      }
      responder.sendString(GSON.toJson(new ServiceResponse<>(store.listWorkspaces(ns))));
    });
  }

  @GET
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void getWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("context") String namespace,
                           @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Getting workspace in system namespace is currently not supported");
      }
      responder.sendString(GSON.toJson(store.getWorkspace(new WorkspaceId(ns, workspaceId))));
    });
  }

  /**
   * Update the workspace
   */
  @POST
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
      Workspace newWorkspace = Workspace.builder(store.getWorkspace(wsId))
                                 .setDirectives(updateRequest.getDirectives())
                                 .setInsights(updateRequest.getInsights())
                                 .setUpdatedTimeMillis(System.currentTimeMillis()).build();
      store.updateWorkspace(wsId, newWorkspace);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @DELETE
  @Path("v2/contexts/{context}/workspaces/{id}")
  public void deleteWorkspace(HttpServiceRequest request, HttpServiceResponder responder,
                              @PathParam("context") String namespace,
                              @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException("Deleting workspace in system namespace is currently not supported");
      }
      store.deleteWorkspace(new WorkspaceId(ns, workspaceId));
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  /**
   * Upload data to the workspace, the workspace is created automatically on fly.
   */
  @POST
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
      store.saveWorkspace(id, new WorkspaceDetail(workspace, sample));
      responder.sendJson(id.getWorkspaceId());
    });
  }

  /**
   * Executes the directives on the record.
   */
  @POST
  @Path("v2/contexts/{context}/workspaces/{id}/execute")
  public void execute(HttpServiceRequest request, HttpServiceResponder responder,
                      @PathParam("context") String namespace,
                      @PathParam("id") String workspaceId) {
    respond(responder, namespace, ns -> {
      if (ns.getName().equalsIgnoreCase(NamespaceId.SYSTEM.getNamespace())) {
        throw new BadRequestException(
          "Executing directives in system namespace is currently not supported");
      }

      DirectiveExecutionRequest executionRequest =
        GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(),
                      DirectiveExecutionRequest.class);
      WorkspaceId wsId = new WorkspaceId(ns, workspaceId);
      WorkspaceDetail detail = store.getWorkspaceDetail(wsId);
      List<Row> result = getContext().isRemoteTaskEnabled() ?
        executeRemotely(ns.getName(), executionRequest.getDirectives(), detail) :
        executeLocally(ns.getName(), executionRequest.getDirectives(), detail);
      DirectiveExecutionResponse response = generateExecutionResponse(result,
                                                                      executionRequest.getLimit());
      Workspace newWorkspace = Workspace.builder(detail.getWorkspace())
                                 .setDirectives(executionRequest.getDirectives())
                                 .setUpdatedTimeMillis(System.currentTimeMillis()).build();
      store.updateWorkspace(wsId, newWorkspace);
      responder.sendJson(response);
    });
  }

  private List<Row> executeLocally(String namespace, List<String> directives,
                                   WorkspaceDetail detail) throws DirectiveLoadException, DirectiveParseException {
    // load the udd
    composite.reload(namespace);
    return executeDirectives(namespace, directives, new ArrayList<>(detail.getSample()));
  }


  private List<Row> executeRemotely(String namespace, List<String> directives,
                                    WorkspaceDetail detail) throws Exception {
    RemoteDirectiveRequest directiveRequest = new RemoteDirectiveRequest(directives,
                                                                         namespace, detail.getSampleAsBytes());
    RunnableTaskRequest runnableTaskRequest = RunnableTaskRequest.getBuilder(RemoteExecutionTask.class.getName()).
      withParam(GSON.toJson(directiveRequest)).build();
    byte[] bytes = getContext().runTask(runnableTaskRequest);
    return new ObjectSerDe<List<Row>>().toObject(bytes);
  }

  /**
   * Retrieve the directives available in the namespace
   */
  @GET
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
      WorkspaceDetail detail = store.getWorkspaceDetail(wsId);
      List<String> directives = detail.getWorkspace().getDirectives();
      List<Row> result = getContext().isRemoteTaskEnabled() ?
        executeRemotely(ns.getName(), directives, detail) :
        executeLocally(ns.getName(), directives, detail);

      SchemaConverter schemaConvertor = new SchemaConverter();
      Schema schema = schemaConvertor.toSchema("record", createUberRecord(result));
      Map<String, String> properties = ImmutableMap.of("directives", String.join("\n", directives));

      Set<StageSpec> srcSpecs = getSourceSpecs(detail, directives);

      ArtifactSummary wrangler = composite.getLatestWranglerArtifact();
      responder.sendString(GSON.toJson(new WorkspaceSpec(
        srcSpecs, new StageSpec(
          schema, new Plugin("Wrangler", "transform", properties,
                             wrangler == null ? null :
                               new Artifact(wrangler.getName(), wrangler.getVersion(),
                                            wrangler.getScope().name().toLowerCase()))))));
    });
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
}
