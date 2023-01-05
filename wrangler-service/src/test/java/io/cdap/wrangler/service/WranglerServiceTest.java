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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.internal.io.SchemaTypeAdapter;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.ServiceManager;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.wrangler.DataPrep;
import io.cdap.wrangler.service.directive.DirectivesHandler;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.URL;
import java.net.URLEncoder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link DirectivesHandler}.
 */
@Ignore
public class WranglerServiceTest extends WranglerServiceTestBase {
  private static final Gson GSON =
    new GsonBuilder().registerTypeAdapter(Schema.class, new SchemaTypeAdapter()).create();

  @Test
  public void test() throws Exception {
    ApplicationManager wrangerApp = deployApplication(DataPrep.class);
    ServiceManager serviceManager = wrangerApp.getServiceManager("service").start();
    // should throw exception, instead of returning null
    URL baseURL = serviceManager.getServiceURL();


    List<String> uploadContents = ImmutableList.of("bob,anderson", "joe,mchall");
    createAndUploadWorkspace(baseURL, "test_ws", uploadContents);

    List<String> directives =
      ImmutableList.of("split-to-columns test_ws ,",
                       "drop test_ws",
                       "rename test_ws_1 fname",
                       "rename test_ws_2 lname");

    Schema schema = schema(baseURL, "test_ws", directives);

    Schema expectedSchema =
      Schema.recordOf("avroSchema",
                      Schema.Field.of("fname", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                      Schema.Field.of("lname", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    Assert.assertEquals(expectedSchema, schema);
    serviceManager.stop();
  }

  public Schema schema(URL baseURL, String workspace, List<String> directives) throws Exception {
    List<Map.Entry<String, String>> queryParams = new ArrayList<>();
    for (String directive : directives) {
      queryParams.add(new AbstractMap.SimpleEntry<>("directive", URLEncoder.encode(directive, "UTF-8")));
    }

    URL url = new URL(baseURL, "workspaces/" + workspace + "/schema" + createQueryParams(queryParams));
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(200, response.getResponseCode());

    // we have to do this, simply because of how the service REST API returns only the Fields of the Schema
    return GSON.fromJson("{ \"name\": \"avroSchema\", \"type\": \"record\", \"fields\":"
                           + response.getResponseBodyAsString() + " }", Schema.class);
  }
}
