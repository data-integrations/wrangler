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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.wrangler.WranglerApp;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TableLookupTest extends HydratorTestBase {

  private static final class ExecuteResponse {
    List<String> headers;
    String message;
    int items;
    List<Map<String, String>> value;
    int status;
  }

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


    ApplicationManager wrangerApp = deployApplication(WranglerApp.class);
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

  private void createAndUploadWorkspace(URL baseURL, String workspace, List<String> lines) throws Exception {
    HttpResponse response = HttpRequests.execute(HttpRequest.put(new URL(baseURL, "workspaces/" + workspace)).build());
    Assert.assertEquals(200, response.getResponseCode());
    response = HttpRequests.execute(
      HttpRequest.post(new URL(baseURL, "workspaces/" + workspace +"/upload"))
        .withBody(Joiner.on(URLEncoder.encode("\n", "UTF-8")).join(lines))
        .addHeader("recorddelimiter", URLEncoder.encode("\n", "UTF-8"))
        .build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private ExecuteResponse execute(URL baseURL, String workspace, String[] directives) throws Exception {
    String directivesHeader = "";
    for (String directive : directives) {
      directivesHeader += "&directive=" + URLEncoder.encode(directive, "UTF-8");
    }
    URL url = new URL(baseURL, "workspaces/" + workspace + "/execute?limit=10" + directivesHeader);
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(200, response.getResponseCode());
    return new Gson().fromJson(response.getResponseBodyAsString(), ExecuteResponse.class);
  }
}
