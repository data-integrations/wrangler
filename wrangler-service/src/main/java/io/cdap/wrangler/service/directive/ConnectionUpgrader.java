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
import io.cdap.cdap.api.service.SystemServiceContext;
import io.cdap.cdap.etl.proto.connection.ConnectionCreationRequest;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.proto.ConflictException;
import io.cdap.wrangler.proto.Namespace;
import io.cdap.wrangler.proto.connection.Connection;
import io.cdap.wrangler.proto.connection.ConnectionType;
import io.cdap.wrangler.store.upgrade.UpgradeEntityType;
import io.cdap.wrangler.store.upgrade.UpgradeState;
import io.cdap.wrangler.store.upgrade.UpgradeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Upgrader for connections
 */
public class ConnectionUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionUpgrader.class);
  private static final UpgradeState CONN_COMPLETE_STATE = new UpgradeState(1L);

  private final UpgradeStore upgradeStore;
  private final SystemServiceContext context;
  private final long upgradeBeforeTsSecs;
  private final ConnectorArtifactLoader artifactLoader;
  private final ConnectionDiscoverer discoverer;

  public ConnectionUpgrader(UpgradeStore upgradeStore, SystemServiceContext context, long upgradeBeforeTsSecs) {
    this.upgradeStore = upgradeStore;
    this.context = context;
    this.upgradeBeforeTsSecs = upgradeBeforeTsSecs;
    this.artifactLoader = new ConnectorArtifactLoader(context);
    this.discoverer = new ConnectionDiscoverer(context);
  }

  public void upgradeConnections() throws Exception {
    List<NamespaceSummary> namespaces = context.listNamespaces();
    for (NamespaceSummary ns : namespaces) {
      UpgradeState state = upgradeStore.getEntityUpgradeState(ns, UpgradeEntityType.CONNECTION);
      if (state == null || state.getVersion() == 0L) {
        upgradeConnectionsInNamespace(ns);
      }
    }
    upgradeStore.setEntityUpgradeState(UpgradeEntityType.CONNECTION, CONN_COMPLETE_STATE);
  }

  private void upgradeConnectionsInNamespace(NamespaceSummary namespace) {
    List<Connection> connections = TransactionRunners.run(context, ctx -> {
      ConnectionStore connStore = ConnectionStore.get(ctx);
      return connStore.list(new Namespace(namespace.getName(), namespace.getGeneration()),
                            connection -> connection.getCreated() < upgradeBeforeTsSecs);
    });

    for (Connection connection : connections) {
      // do not upgrade pre configured connection
      if (connection.isPreconfigured()) {
        continue;
      }

      // if it is not upgradable
      ConnectionType type = connection.getType();
      if (!ConnectionType.CONN_UPGRADABLE_TYPES.contains(type)) {
        continue;
      }

      PluginInfo pluginInfo = artifactLoader.getPluginInfo(type.getConnectorName());
      if (pluginInfo == null) {
        LOG.warn("Unable to find the connector for connection type {} with connection name {}, " +
                   "upgrade will not be done for it", type.name().toLowerCase(), connection.getName());
        continue;
      }

      PluginInfo info = new PluginInfo(
        pluginInfo.getName(), pluginInfo.getType(), pluginInfo.getCategory(),
        SpecificationUpgradeUtils.getConnectorProperties(type, connection.getProperties()),
        pluginInfo.getArtifact());
      ConnectionCreationRequest request = new ConnectionCreationRequest(connection.getDescription(), info);
      try {
        discoverer.addConnection(namespace.getName(), connection.getName(), request);
      } catch (ConflictException e) {
        // if there is a conflict exception, it is quite possible this connection is already upgraded before
        LOG.debug("A connection {} already exists, ignoring the upgrade", connection.getName());
      } catch (Exception e) {
        LOG.warn("Failed to upgrade connection {}", connection.getName(), e);
      }
    }
    upgradeStore.setEntityUpgradeState(namespace, UpgradeEntityType.CONNECTION, CONN_COMPLETE_STATE);
  }
}
