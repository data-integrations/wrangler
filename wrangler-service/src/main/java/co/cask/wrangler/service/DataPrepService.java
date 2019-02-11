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

package co.cask.wrangler.service;

import co.cask.cdap.api.service.AbstractSystemService;
import co.cask.wrangler.dataset.connections.ConnectionStore;
import co.cask.wrangler.dataset.schema.SchemaRegistry;
import co.cask.wrangler.dataset.workspace.ConfigStore;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.bigquery.BigQueryHandler;
import co.cask.wrangler.service.connections.ConnectionHandler;
import co.cask.wrangler.service.connections.ConnectionTypeConfig;
import co.cask.wrangler.service.database.DatabaseHandler;
import co.cask.wrangler.service.directive.DirectivesHandler;
import co.cask.wrangler.service.explorer.FilesystemExplorer;
import co.cask.wrangler.service.gcs.GCSHandler;
import co.cask.wrangler.service.kafka.KafkaHandler;
import co.cask.wrangler.service.s3.S3Handler;
import co.cask.wrangler.service.schema.SchemaRegistryHandler;
import co.cask.wrangler.service.spanner.SpannerHandler;

/**
 * Data prep service.
 */
public class DataPrepService extends AbstractSystemService {
  private final ConnectionTypeConfig config;

  public DataPrepService(ConnectionTypeConfig config) {
    this.config = config;
  }

  @Override
  protected void configure() {
    setName("service");

    createTable(ConfigStore.TABLE_SPEC);
    createTable(SchemaRegistry.META_TABLE_SPEC);
    createTable(SchemaRegistry.ENTRY_TABLE_SPEC);
    createTable(ConnectionStore.TABLE_SPEC);
    createTable(WorkspaceDataset.TABLE_SPEC);

    addHandler(new DirectivesHandler());
    addHandler(new SchemaRegistryHandler());
    addHandler(new FilesystemExplorer());
    addHandler(new ConnectionHandler(config));
    addHandler(new KafkaHandler());
    addHandler(new DatabaseHandler());
    addHandler(new S3Handler());
    addHandler(new GCSHandler());
    addHandler(new BigQueryHandler());
    addHandler(new SpannerHandler());
  }
}
