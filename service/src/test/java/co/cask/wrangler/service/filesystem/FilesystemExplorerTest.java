package co.cask.wrangler.service.filesystem;

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.wrangler.service.explorer.DatasetProvider;
import co.cask.wrangler.service.explorer.Explorer;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

/**
 * Class description here.
 */
@Ignore
public class FilesystemExplorerTest extends TestBase {
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @Test
  public void testExplorer() throws Exception {
    ApplicationManager app = deployApplication(TestApp.class);
    ServiceManager service = app.getServiceManager("service");
    service.start();
    Explorer explorer = new Explorer(new DatasetProvider() {
      @Override
      public Dataset acquire() throws Exception {
        return (FileSet) getDataset("indexds").get();
      }

      @Override
      public void release(Dataset dataset) {
      }
    });
    Map<String, Object> listing = explorer.browse("/", false);
    Assert.assertTrue(listing.size() > 0);
    service.stop();
  }
}