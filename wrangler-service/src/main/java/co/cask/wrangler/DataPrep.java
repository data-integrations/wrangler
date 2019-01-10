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

package co.cask.wrangler;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.dataset.schema.SchemaRegistry;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Wrangler Application.
 */
public class DataPrep extends AbstractApplication<ConnectionTypeConfig> {
  public static final String CONNECTIONS_DATASET = "connections";

  /**
   * Override this method to declare and configure the application.
   */
  @Override
  public void configure() {
    setName("dataprep");
    setDescription("DataPrep Backend Service");

    createDataset(WorkspaceDataset.DATASET_NAME, Table.class,
                  DatasetProperties.builder().setDescription("DataPrep workspace dataset.").build());
    createDataset(CONNECTIONS_DATASET, Table.class,
                  DatasetProperties.builder().setDescription("DataPrep connections store.").build());
    createDataset(SchemaRegistry.DATASET_NAME, Table.class,
                  DatasetProperties.builder().setDescription("DataPrep schema registry.").build());

    // Used by the file service.
    createDataset("dataprepfs", FileSet.class, FileSetProperties.builder()
      .setBasePath("dataprepfs/indexds")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setDescription("Store Dataprep Index files")
      .build());

    addService("service",
               new DirectivesHandler(),
               new SchemaRegistryHandler(),
               new FilesystemExplorer(),
               new ConnectionHandler(getConfig()),
               new KafkaHandler(),
               new DatabaseHandler(),
               new S3Handler(),
               new GCSHandler(),
               new BigQueryHandler(),
               new SpannerHandler()
    );
  }
}
