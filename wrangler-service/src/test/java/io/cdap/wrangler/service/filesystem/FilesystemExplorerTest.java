/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.filesystem;

import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.cdap.test.TestBase;
import io.cdap.wrangler.service.explorer.DatasetProvider;
import io.cdap.wrangler.service.explorer.Explorer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

/**
 * Class description here.
 */
@Ignore
public class FilesystemExplorerTest extends TestBase {
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
