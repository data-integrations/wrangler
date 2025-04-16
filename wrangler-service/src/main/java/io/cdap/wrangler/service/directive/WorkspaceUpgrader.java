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

import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.service.SystemServiceContext;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.ConnectorDetail;
import io.cdap.cdap.etl.proto.connection.PluginDetail;
import io.cdap.cdap.etl.proto.connection.SpecGenerationRequest;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.PropertyIds;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.workspace.Workspace;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.NamespacedId;
import io.cdap.wrangler.proto.NotFoundException;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.proto.workspace.v2.Artifact;
import io.cdap.wrangler.proto.workspace.v2.Plugin;
import io.cdap.wrangler.proto.workspace.v2.SampleSpec;
import io.cdap.wrangler.proto.workspace.v2.StageSpec;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceDetail;
import io.cdap.wrangler.proto.workspace.v2.WorkspaceId;
import io.cdap.wrangler.service.database.DatabaseHandler;
import io.cdap.wrangler.store.upgrade.UpgradeEntityType;
import io.cdap.wrangler.store.upgrade.UpgradeState;
import io.cdap.wrangler.store.upgrade.UpgradeStore;
import io.cdap.wrangler.store.workspace.WorkspaceStore;
import io.cdap.wrangler.utils.RecordConvertorException;
import io.cdap.wrangler.utils.RowHelper;
import io.cdap.wrangler.utils.SchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Upgrader for workspaces. This upgrade should be run after the {@link ConnectionUpgrader}
 */
public class WorkspaceUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(WorkspaceUpgrader.class);
  private static final UpgradeState WS_COMPLETE_STATE = new UpgradeState(1L);

  private final UpgradeStore upgradeStore;
  private final SystemServiceContext context;
  private final long upgradeBeforeTsSecs;
  private final ConnectionDiscoverer discoverer;
  private final WorkspaceStore wsStore;

  public WorkspaceUpgrader(UpgradeStore upgradeStore, SystemServiceContext context, long upgradeBeforeTsSecs,
                           WorkspaceStore wsStore) {
    this.upgradeStore = upgradeStore;
    this.context = context;
    this.upgradeBeforeTsSecs = upgradeBeforeTsSecs;
    this.discoverer = new ConnectionDiscoverer(context);
    this.wsStore = wsStore;
  }

  public void upgradeWorkspaces() throws Exception {
    List<NamespaceSummary> namespaces = context.listNamespaces();
    for (NamespaceSummary ns : namespaces) {
      UpgradeState state = upgradeStore.getEntityUpgradeState(ns, UpgradeEntityType.WORKSPACE);
      if (state == null || state.getVersion() == 0L) {
        upgradeWorkspacesInConnections(ns);
      }
    }
    upgradeStore.setEntityUpgradeState(UpgradeEntityType.WORKSPACE, WS_COMPLETE_STATE);
  }

  private void upgradeWorkspacesInConnections(NamespaceSummary namespace) {
    List<Workspace> workspaces = TransactionRunners.run(context, ctx -> {
      WorkspaceDataset wsDataset = WorkspaceDataset.get(ctx);
      return wsDataset.listWorkspaces(namespace, upgradeBeforeTsSecs);
    });

    for (Workspace workspace : workspaces) {
      List<Row> sample = Collections.emptyList();
      try {
        sample = DirectivesHandler.fromWorkspace(workspace);
      } catch (Exception e) {
        // this should not happen, but guard here to avoid failing the entire upgrade process
        LOG.warn("Could not decode the sample data for workspace {}. This workspace will not be upgraded",
                 workspace.getName(), e);
      }
      List<String> directives = workspace.getRequest() == null ? Collections.emptyList() :
                                  workspace.getRequest().getRecipe().getDirectives();
      WorkspaceId workspaceId = new WorkspaceId(namespace, workspace.getNamespacedId().getId());

      long now = System.currentTimeMillis();
      io.cdap.wrangler.proto.workspace.v2.Workspace.Builder ws =
        io.cdap.wrangler.proto.workspace.v2.Workspace
          .builder(workspace.getName(), workspace.getNamespacedId().getId()).setDirectives(directives)
          .setCreatedTimeMillis(now).setUpdatedTimeMillis(now);

      ConnectionType connectionType =
        ConnectionType.valueOf(workspace.getProperties().get(PropertyIds.CONNECTION_TYPE).toUpperCase());
      // if it is not upgradable workspace types, just ignore and continue, i.e, ADLS
      if (!ConnectionType.WORKSPACE_UPGRADABLE_TYPES.contains(connectionType)) {
        LOG.debug("Workspace {} of type {} is not upgradable. This workspace will not be upgraded",
                  workspace.getName(), connectionType);
        continue;
      }

      // if this type is not related to any connectors, just save it with empty sample sepc,
      // so there will be no sources created when converting to pipeline,
      // for now the only type like this is UPLOAD
      if (!ConnectionType.CONN_UPGRADABLE_TYPES.contains(connectionType)) {
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.build(), sample));
        continue;
      }

      String connectorName = connectionType.getConnectorName();
      String path = SpecificationUpgradeUtils.getPath(connectionType, workspace);

      String connection = workspace.getProperties().get(PropertyIds.CONNECTION_ID);
      try {
        ConnectorDetail detail = discoverer.getSpecification(namespace.getName(), connection,
                                                             new SpecGenerationRequest(path, Collections.emptyMap()));

        // special handle of the database type, the current db connector supports browsing from schema/table depending
        // on the database type, here it is safe to just use the old way to generate spec since:
        // 1. the old db connection might not contain database in connection string
        // 2. it does not support schema, so db type which supports schema can easily get messed up due to the path
        //    not set up correctly. For example, for mysql, the getspec call will return the source schema since it
        //    mysql itself does not support schema. But SqlServer will not return source schema because it supports
        //    schema and the original workspace does not have this information. To make sure, the path does not mess up,
        //    the above path for db connection will always be "/".
        // 3. Using the old properties will ensure the old behavior get reserved for the db connectors
        Schema dbSchema = null;
        Map<String, String> dbProperties = Collections.emptyMap();
        if (connectionType.equals(ConnectionType.DATABASE)) {
          SchemaConverter schemaConvertor = new SchemaConverter();
          try {
            dbSchema = sample.isEmpty() ? null :
                         schemaConvertor.toSchema("record", RowHelper.createMergedRow(sample));
          } catch (RecordConvertorException e) {
            LOG.warn("Unable to get the source schema for workspace {}, the generated spec will not contain schema.",
                     workspace.getName());
          }
          Connection conn = TransactionRunners.run(context, ctx -> {
            ConnectionStore connStore = ConnectionStore.get(ctx);
            return connStore.get(new NamespacedId(new Namespace(namespace.getName(), namespace.getGeneration()),
                                                  connection));
          });
          dbProperties = DatabaseHandler.getSpecification(conn, workspace.getName());
        }

        Set<StageSpec> relatedPlugins = new HashSet<>();
        for (PluginDetail plugin : detail.getRelatedPlugins()) {
          Schema schema = connectionType.equals(ConnectionType.DATABASE) ? dbSchema : plugin.getSchema();
          Map<String, String> properties =
            connectionType.equals(ConnectionType.DATABASE) ? dbProperties : plugin.getProperties();
          ArtifactSelectorConfig artifact = plugin.getArtifact();
          Plugin pluginSpec = new Plugin(
            plugin.getName(), plugin.getType(), properties,
            new Artifact(artifact.getName(), artifact.getVersion(), artifact.getScope()));
          relatedPlugins.add(new StageSpec(schema, pluginSpec));
        }
        SampleSpec spec = new SampleSpec(connection, connectorName, path, relatedPlugins);
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.setSampleSpec(spec).build(), sample));
      } catch (NotFoundException e) {
        LOG.warn("Connection {} related to workspace {} does not exist. " +
                   "The workspace will be upgraded without that information", connection, workspace.getName());
        wsStore.saveWorkspace(workspaceId, new WorkspaceDetail(ws.build(), sample));
      } catch (Exception e) {
        LOG.warn("Unable to get the spec from connection {} for workspace {}. The workspace will not be upgraded.",
                 connection, workspace.getName());
      }
    }

    upgradeStore.setEntityUpgradeState(namespace, UpgradeEntityType.WORKSPACE, WS_COMPLETE_STATE);
  }
}
