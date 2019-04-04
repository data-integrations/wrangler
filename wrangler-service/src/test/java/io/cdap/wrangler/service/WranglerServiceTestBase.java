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

import com.google.gson.Gson;
import io.cdap.cdap.test.TestBase;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequests;
import io.cdap.common.http.HttpResponse;
import io.cdap.wrangler.service.directive.DirectivesHandler;
import org.junit.Assert;
import org.junit.Ignore;

import java.net.URL;
import java.net.URLEncoder;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class which exposes utility functions for interacting with {@link DirectivesHandler}.
 */
@Ignore
public class WranglerServiceTestBase extends TestBase {

  protected static final class ExecuteResponse {
    public List<String> headers;
    public String message;
    public int items;
    public List<Map<String, String>> value;
    public int status;
  }

  protected void createAndUploadWorkspace(URL baseURL, String workspace, List<String> lines) throws Exception {
    HttpResponse response = HttpRequests.execute(HttpRequest.put(new URL(baseURL, "workspaces/" + workspace)).build());
    Assert.assertEquals(200, response.getResponseCode());
//    response = HttpRequests.execute(
//      HttpRequest.post(new URL(baseURL, "workspaces/" + workspace +"/upload"))
//        .withBody(Joiner.on(URLEncoder.encode("\n", "UTF-8")).join(lines))
//        .addHeader("recorddelimiter", URLEncoder.encode("\n", "UTF-8"))
//        .build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  protected ExecuteResponse execute(URL baseURL, String workspace, String[] directives) throws Exception {
    List<Map.Entry<String, String>> queryParams = new ArrayList<>();
    for (String directive : directives) {
      queryParams.add(new AbstractMap.SimpleEntry<>("directive", URLEncoder.encode(directive, "UTF-8")));
    }
    queryParams.add(new AbstractMap.SimpleEntry<>("limit", "100"));

    URL url = new URL(baseURL, "workspaces/" + workspace + "/execute" + createQueryParams(queryParams));
    HttpResponse response = HttpRequests.execute(HttpRequest.get(url).build());
    Assert.assertEquals(200, response.getResponseCode());
    return new Gson().fromJson(response.getResponseBodyAsString(), ExecuteResponse.class);
  }

  // returns the query params string, including any leading '?'
  protected String createQueryParams(List<Map.Entry<String, String>> queryParams) {
    if (queryParams.isEmpty()) {
      return "";
    }
    String params = "?" + queryParams.get(0).getKey() + "=" + queryParams.get(0).getValue();
    for (int i = 1; i < queryParams.size(); i++) {
      params += "&" + queryParams.get(i).getKey() + "=" + queryParams.get(i).getValue();
    }
    return params;
  }
}
