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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.service.directive.DirectivesService;
import co.cask.wrangler.service.filesystem.FSBrowserService;
import co.cask.wrangler.service.recipe.RecipeDatum;
import co.cask.wrangler.service.recipe.RecipeService;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Wrangler Application.
 */
public class DataPrep extends AbstractApplication {
  /**
   * Override this method to declare and configure the application.
   */
  @Override
  public void configure() {
    setName("dataprep");
    setDescription("DataPrep Backend Service");
    createDataset(DirectivesService.WORKSPACE_DATASET, Table.class,
                  DatasetProperties.builder().setDescription("DataPrep Dataset").build());
    try {
      createDataset(RecipeService.DATASET, ObjectMappedTable.class,
                    ObjectMappedTableProperties.builder()
                    .setType(RecipeDatum.class).build());
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("Unable to create Recipe store dataset. " + e.getMessage());
    }

    // Used by the file service.
    createDataset("lines", FileSet.class, FileSetProperties.builder()
      .setBasePath("example/data/lines")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setDescription("Store input lines")
      .build());

    addService("service",
               new DirectivesService(),
               new RecipeService(),
               new FSBrowserService()
    );

  }
}
