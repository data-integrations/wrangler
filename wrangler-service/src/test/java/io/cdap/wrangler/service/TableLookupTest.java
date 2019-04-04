/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.service;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.wrangler.DataPrep;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
import java.util.List;

/**
 * Tests {@link io.cdap.wrangler.steps.transformation.TableLookup}.
 */
@Ignore
public class TableLookupTest extends WranglerServiceTestBase {

  @Test
  public void test() throws Exception {
    // setup lookup data
    addDatasetInstance("table", "lookupTable");
    DataSetManager<Table> lookupTable = getDataset("lookupTable");
    lookupTable.get().put(Bytes.toBytes("bob"), Bytes.toBytes("age"), Bytes.toBytes("21"));
    lookupTable.get().put(Bytes.toBytes("bob"), Bytes.toBytes("city"), Bytes.toBytes("Los Angeles, CA"));
    lookupTable.get().put(Bytes.toBytes("joe"), Bytes.toBytes("age"), Bytes.toBytes("34"));
    lookupTable.get().put(Bytes.toBytes("joe"), Bytes.toBytes("city"), Bytes.toBytes("Palo Alto, CA"));
    lookupTable.flush();


    ApplicationManager wrangerApp = deployApplication(DataPrep.class);
    ServiceManager serviceManager = wrangerApp.getServiceManager("service").start();
    // should throw exception, instead of returning null
    URL baseURL = serviceManager.getServiceURL();

    List<String> uploadContents = ImmutableList.of("bob,anderson", "joe,mchall");
    createAndUploadWorkspace(baseURL, "test_ws", uploadContents);

    String[] directives = new String[]{
      "split-to-columns test_ws ,",
      "drop test_ws",
      "rename test_ws_1 fname",
      "rename test_ws_2 lname",
      "table-lookup fname lookupTable"
    };

    ExecuteResponse executeResponse = execute(baseURL, "test_ws", directives);
    Assert.assertEquals(uploadContents.size(), executeResponse.value.size());
    Assert.assertEquals("bob", executeResponse.value.get(0).get("fname"));
    Assert.assertEquals("21", executeResponse.value.get(0).get("fname_age"));
    Assert.assertEquals("Los Angeles, CA", executeResponse.value.get(0).get("fname_city"));
    Assert.assertEquals("joe", executeResponse.value.get(1).get("fname"));
    Assert.assertEquals("34", executeResponse.value.get(1).get("fname_age"));
    Assert.assertEquals("Palo Alto, CA", executeResponse.value.get(1).get("fname_city"));
  }
}
