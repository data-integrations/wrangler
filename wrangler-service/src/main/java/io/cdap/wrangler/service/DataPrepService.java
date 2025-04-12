/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.service;

import io.cdap.cdap.api.service.AbstractSystemService;
import io.cdap.cdap.api.service.SystemServiceContext;
import io.cdap.wrangler.dataset.connections.ConnectionStore;
import io.cdap.wrangler.dataset.schema.SchemaRegistry;
import io.cdap.wrangler.dataset.workspace.ConfigStore;
import io.cdap.wrangler.dataset.workspace.WorkspaceDataset;
import io.cdap.wrangler.service.adls.ADLSHandler;
import io.cdap.wrangler.service.bigquery.BigQueryHandler;
import io.cdap.wrangler.service.connections.ConnectionHandler;
import io.cdap.wrangler.service.connections.ConnectionTypeConfig;
import io.cdap.wrangler.service.database.DatabaseHandler;
import io.cdap.wrangler.service.directive.ConnectionUpgrader;
import io.cdap.wrangler.service.directive.DirectivesHandler;
import io.cdap.wrangler.service.directive.RecipeHandler;
import io.cdap.wrangler.service.directive.WorkspaceHandler;
import io.cdap.wrangler.service.directive.WorkspaceUpgrader;
import io.cdap.wrangler.service.explorer.FilesystemExplorer;
import io.cdap.wrangler.service.gcs.GCSHandler;
import io.cdap.wrangler.service.kafka.KafkaHandler;
import io.cdap.wrangler.service.s3.S3Handler;
import io.cdap.wrangler.service.schema.DataModelHandler;
import io.cdap.wrangler.service.schema.SchemaRegistryHandler;
import io.cdap.wrangler.service.spanner.SpannerHandler;
import io.cdap.wrangler.store.recipe.RecipeStore;
import io.cdap.wrangler.store.upgrade.UpgradeEntityType;
import io.cdap.wrangler.store.upgrade.UpgradeState;
import io.cdap.wrangler.store.upgrade.UpgradeStore;
import io.cdap.wrangler.store.workspace.WorkspaceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Data prep service.
 */
public class DataPrepService extends AbstractSystemService {
  private static final Logger LOG = LoggerFactory.getLogger(DataPrepService.class);
  private static final UpgradeState PRE_UPGRADE = new UpgradeState(0L);

  private final ConnectionTypeConfig config;

  public DataPrepService(ConnectionTypeConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    setName("service");

    createTable(ConfigStore.TABLE_SPEC);
    createTable(ConnectionStore.TABLE_SPEC);
    createTable(SchemaRegistry.META_TABLE_SPEC);
    createTable(SchemaRegistry.ENTRY_TABLE_SPEC);
    createTable(WorkspaceDataset.TABLE_SPEC);
    createTable(WorkspaceStore.WORKSPACE_TABLE_SPEC);
    createTable(UpgradeStore.UPGRADE_TABLE_SPEC);
    createTable(RecipeStore.RECIPE_TABLE_SPEC);

    addHandler(new DirectivesHandler());
    addHandler(new SchemaRegistryHandler());
    addHandler(new FilesystemExplorer());
    addHandler(new ConnectionHandler(config));
    addHandler(new KafkaHandler());
    addHandler(new DatabaseHandler());
    addHandler(new S3Handler());
    addHandler(new GCSHandler());
    addHandler(new ADLSHandler());
    addHandler(new BigQueryHandler());
    addHandler(new SpannerHandler());
    addHandler(new DataModelHandler());
    addHandler(new WorkspaceHandler());
    addHandler(new RecipeHandler());
  }

  @Override
  public void initialize(SystemServiceContext context) {
    // only do the upgrade on first instance to avoid transaction conflict
    if (context.getInstanceId() != 0) {
      return;
    }

    UpgradeStore upgradeStore = new UpgradeStore(context);
    WorkspaceStore wsStore = new WorkspaceStore(context);
    UpgradeState connState = upgradeStore.getEntityUpgradeState(UpgradeEntityType.CONNECTION);
    UpgradeState wsState = upgradeStore.getEntityUpgradeState(UpgradeEntityType.WORKSPACE);
    boolean isConnDone = connState != null && connState.getVersion() == 1L;
    boolean isWsDone = wsState != null && wsState.getVersion() == 1L;
    if (isConnDone && isWsDone) {
      return;
    }

    long timestampNowMillis = System.currentTimeMillis();
    long upgradeBefore =
      upgradeStore.initializeAndRetrieveUpgradeTimestampMillis(UpgradeEntityType.CONNECTION, timestampNowMillis,
                                                               PRE_UPGRADE);
    upgradeStore.initializeAndRetrieveUpgradeTimestampMillis(UpgradeEntityType.WORKSPACE, timestampNowMillis,
                                                             PRE_UPGRADE);
    long upgradeBeforeTsSecs = TimeUnit.MILLISECONDS.toSeconds(upgradeBefore);
    if (!isConnDone) {
      try {
        ConnectionUpgrader connectionUpgrader = new ConnectionUpgrader(upgradeStore, context, upgradeBeforeTsSecs);
        connectionUpgrader.upgradeConnections();
      } catch (Exception e) {
        // check if there is any error upgrading the connections, if true, no point to continue upgrading the workspace
        // as most connections won't be able to get the spec.
        // also we don't want the service fail to start due to upgrade failure
        LOG.error("Failed to upgrade the connections", e);
        return;
      }
    }

    if (!isWsDone) {
      try {
        WorkspaceUpgrader workspaceUpgrader =
          new WorkspaceUpgrader(upgradeStore, context, upgradeBeforeTsSecs, wsStore);
        workspaceUpgrader.upgradeWorkspaces();
      } catch (Exception e) {
        // don't want the service fail to start due to upgrade failure
        LOG.error("Failed to upgrade the workspaces", e);
      }
    }
  }
}
