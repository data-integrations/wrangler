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

import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactManager;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.proto.ArtifactSelectorConfig;
import io.cdap.cdap.etl.proto.connection.PluginInfo;
import io.cdap.wrangler.utils.ArtifactSummaryComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Connector artifact loader
 */
public class ConnectorArtifactLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectorArtifactLoader.class);
  private static final Map<String, String> CONNECTORS_PLUGINS_MAP;

  static {
    Map<String, String> connectors = new HashMap<>();
    connectors.put("BigQuery", "google-cloud");
    connectors.put("GCS", "google-cloud");
    connectors.put("Spanner", "google-cloud");
    connectors.put("Database", "database-plugins");
    connectors.put("S3", "amazon-s3-plugins");
    connectors.put("Kafka", "kafka-plugins");
    CONNECTORS_PLUGINS_MAP = connectors;
  }

  private final ArtifactManager artifactManager;
  // map of connector plugin name -> artifact info
  private final Map<String, PluginInfo> connectors;

  public ConnectorArtifactLoader(ArtifactManager artifactManager) {
    this.artifactManager = artifactManager;
    this.connectors = new HashMap<>();
    reload();
  }

  @Nullable
  public PluginInfo getPluginInfo(String connectorName) {
    return connectors.get(connectorName);
  }

  public void reload() {
    connectors.clear();

    List<ArtifactInfo> artifacts;
    try {
      artifacts = artifactManager.listArtifacts();
    } catch (IOException e) {
      LOG.error("Failed to load connector artifact.", e);
      return;
    }

    ArtifactSummaryComparator comparator = new ArtifactSummaryComparator();
    Map<String, ArtifactSummary> connectorPlugins = new HashMap<>();
    for (ArtifactInfo artifact : artifacts) {
      // for now just support upgrading to system scope artifact to save some logic in finding artifacts in
      // each namespace of connections
      if (artifact.getScope().equals(ArtifactScope.USER)) {
        continue;
      }

      Set<PluginClass> plugins = artifact.getClasses().getPlugins();
      for (PluginClass plugin : plugins) {
        // has to be connector type
        if (!Connector.PLUGIN_TYPE.equalsIgnoreCase(plugin.getType())) {
          continue;
        }

        String name = plugin.getName();
        // has to be one of the migrating connection types
        if (!CONNECTORS_PLUGINS_MAP.containsKey(name)) {
          continue;
        }

        String expectedArtifact = CONNECTORS_PLUGINS_MAP.get(name);
        // artifact name has to match
        if (!expectedArtifact.equals(artifact.getName())) {
          continue;
        }

        // if not latest, continue
        if (connectorPlugins.containsKey(name) && comparator.compare(connectorPlugins.get(name), artifact) > 0) {
          continue;
        }

        PluginInfo info = new PluginInfo(name, Connector.PLUGIN_TYPE, plugin.getCategory(), Collections.emptyMap(),
                                         new ArtifactSelectorConfig(artifact.getScope().name().toLowerCase(),
                                                                    artifact.getName(), artifact.getVersion()));
        connectorPlugins.put(name, artifact);
        connectors.put(name, info);
      }
    }
  }
}
