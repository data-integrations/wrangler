/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.connections.ConnectionService;
import co.cask.wrangler.service.database.DatabaseService;
import co.cask.wrangler.service.directive.DirectivesService;
import co.cask.wrangler.service.explorer.FilesystemExplorer;
import co.cask.wrangler.service.kafka.KafkaService;
import co.cask.wrangler.service.s3.S3Service;
import co.cask.wrangler.service.schema.SchemaRegistryService;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Wrangler Application.
 */
public class DataPrep extends AbstractApplication {
  public static final String DATAPREP_DATASET = "dataprep";

  /**
   * Override this method to declare and configure the application.
   */
  @Override
  public void configure() {
    setName("dataprep");
    setDescription("DataPrep Backend Service");

    createDataset(DirectivesService.WORKSPACE_DATASET, WorkspaceDataset.class,
                  DatasetProperties.builder().setDescription("Dataprep Workspace Management").build());
    createDataset(DATAPREP_DATASET, Table.class,
                  DatasetProperties.builder().setDescription("DataPrep All Store").build());

    // Used by the file service.
    createDataset("dataprepfs", FileSet.class, FileSetProperties.builder()
      .setBasePath("dataprepfs/indexds")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setDescription("Store Dataprep Index files")
      .build());

    addService("service",
               new DirectivesService(),
               new SchemaRegistryService(),
               new FilesystemExplorer(),
               new ConnectionService(),
               new KafkaService(),
               new DatabaseService(),
               new S3Service()
    );
  }
}
