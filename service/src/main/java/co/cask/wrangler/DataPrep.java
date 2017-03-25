package co.cask.wrangler;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.wrangler.service.DirectivesService;

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
    addService("service", new DirectivesService());
  }
}
