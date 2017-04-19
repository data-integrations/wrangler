package co.cask.wrangler.service.filesystem;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.wrangler.DataPrep;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URL;

/**
 * Class description here.
 */
public class FSBrowserServiceTest extends TestBase {
  private static URL base;

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration(Constants.Explore.EXPLORE_ENABLED, false);

  @BeforeClass
  public static void setupTest() throws Exception {
    ApplicationManager wrangerApp = deployApplication(DataPrep.class);
    ServiceManager serviceManager = wrangerApp.getServiceManager("service").start();
    // should throw exception, instead of returning null
    base = serviceManager.getServiceURL();
  }

  @Test
  public void testDirectoryListing() throws Exception {
    HttpResponse response = HttpRequests.execute(
      HttpRequest.put(new URL(base, "explorer?path=/")).build()
    );
    Assert.assertEquals(200, response.getResponseCode());
  }
}