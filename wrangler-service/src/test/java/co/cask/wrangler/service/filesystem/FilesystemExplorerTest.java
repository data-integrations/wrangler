/*
 *  Copyright © 2017-2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

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
