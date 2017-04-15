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
    createDataset("indexds", FileSet.class, FileSetProperties.builder()
      .setBasePath("dataprep/indexds")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setDescription("Store Dataset Index files")
      .build());

    addService("service",
               new DirectivesService(),
               new RecipeService(),
               new FSBrowserService()
    );

  }
}
