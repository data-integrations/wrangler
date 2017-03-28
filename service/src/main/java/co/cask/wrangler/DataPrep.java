package co.cask.wrangler;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.ObjectMappedTable;
import co.cask.cdap.api.dataset.lib.ObjectMappedTableProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.service.directive.DirectivesService;
import co.cask.wrangler.service.recipe.RecipeDatum;
import co.cask.wrangler.service.recipe.RecipeService;

/**
 * Wrangler Application.
 */
public class DataPrep extends AbstractApplication {
  /**
   * Override this method to declare and configure the application.
   */
  @Override
  public void configure() {
    setName("wrangler");
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

    addService("directive", new DirectivesService());
    addService("recipe", new RecipeService());
  }
}
