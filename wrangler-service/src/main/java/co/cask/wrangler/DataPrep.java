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
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.dataset.workspace.WorkspaceDataset;
import co.cask.wrangler.service.bigquery.BigQueryService;
import co.cask.wrangler.service.connections.ConnectionService;
import co.cask.wrangler.service.connections.ConnectionTypeConfig;
import co.cask.wrangler.service.database.DatabaseService;
import co.cask.wrangler.service.directive.DirectivesService;
import co.cask.wrangler.service.explorer.FilesystemExplorer;
import co.cask.wrangler.service.gcs.GCSService;
import co.cask.wrangler.service.kafka.KafkaService;
import co.cask.wrangler.service.recipe.RecipeService;
import co.cask.wrangler.service.s3.S3Service;
import co.cask.wrangler.service.schema.SchemaRegistryService;
import co.cask.wrangler.service.spanner.SpannerService;
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

    createDataset(DirectivesService.WORKSPACE_DATASET, WorkspaceDataset.class,
                  DatasetProperties.builder().setDescription("Dataprep workspace dataset").build());
    createDataset(CONNECTIONS_DATASET, Table.class,
                  DatasetProperties.builder().setDescription("DataPrep connections store.").build());

    Schema schema = Schema.recordOf(
      "recipes",
      Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("description", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("created", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("updated", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("directives", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );

    createDataset(RecipeService.DATASET, Table.class.getName(), DatasetProperties.builder()
      .setDescription("Recipe store.")
      .add(Table.PROPERTY_SCHEMA, schema.toString())
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "id")
      .build());

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
               new ConnectionService(getConfig()),
               new RecipeService(),
               new KafkaService(),
               new DatabaseService(),
               new S3Service(),
               new GCSService(),
               new BigQueryService(),
               new SpannerService()
    );
  }
}
