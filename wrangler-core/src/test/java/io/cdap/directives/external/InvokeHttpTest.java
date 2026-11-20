/*
 *  Copyright © 2019 Cask Data, Inc.
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

package io.cdap.directives.external;

import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.utils.OutboundUrlValidator;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Tests {@link InvokeHttp}
 */
public class InvokeHttpTest {
  private static final Gson GSON = new Gson();
  private static NettyHttpService httpService;
  private static String baseURL;

  @Before
  public void startService() throws Exception {
    System.setProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY, "true");
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new ServiceHandler());
    httpService = NettyHttpService.builder("Services")
      .setHttpHandlers(handlers)
      .build();
    httpService.start();
    int port = httpService.getBindAddress().getPort();
    baseURL = "http://localhost:" + port;
  }

  public static class ServiceHandler implements HttpHandler {
    @Override
    public void init(HandlerContext handlerContext) {
      // no-op
    }

    @Override
    public void destroy(HandlerContext handlerContext) {
      // no-op
    }

    @POST
    @Path("service")
    public void append(FullHttpRequest request, HttpResponder responder,
                       @HeaderParam("C") String headerC) {
      Map<String, Object> object = postRequest(request);
      Map<String, Object> response = new HashMap<>();
      String c = String.format("%s:%f", object.get("a"), object.get("b"));
      response.put("c", c);
      response.put("HeaderC", headerC);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    }

    @POST
    @Path("redirect")
    public void redirect(FullHttpRequest request, HttpResponder responder) {
      HttpHeaders headers = new DefaultHttpHeaders();
      headers.set(HttpHeaderNames.LOCATION, baseURL + "/service");
      responder.sendStatus(HttpResponseStatus.FOUND, headers);
    }

    private Map<String, Object> postRequest(FullHttpRequest request) throws JsonParseException {
      String contentString = request.content().toString(StandardCharsets.UTF_8);
      if (contentString.isEmpty()) {
        return null;
      }
      return new Gson().fromJson(contentString, new TypeToken<Map<String, Object>>() { }.getType());
    }
  }

  @Test
  public void testHttpInvokeWithHeaders() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/service ") + "a,b A=1,B=2,C=3"
    };

    List<Row> rows = Arrays.asList(
      new Row("a", "1").add("b", 2.0),
      new Row("a", "3").add("b", 4.2)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(4, rows.get(0).width());
    Assert.assertEquals(4, rows.get(1).width());
    Assert.assertEquals("1:2.000000", rows.get(0).getValue("c"));
    Assert.assertEquals("3:4.200000", rows.get(1).getValue("c"));
    Assert.assertEquals("3", rows.get(0).getValue("HeaderC"));
    Assert.assertEquals("3", rows.get(1).getValue("HeaderC"));
  }

  @Test
  public void testHttpInvokeWithOutHeaders() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/service ") + "a,b"
    };

    List<Row> rows = Arrays.asList(
      new Row("a", "1").add("b", 2.0),
      new Row("a", "3").add("b", 4.2)
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(2, rows.size());
    Assert.assertEquals("1:2.000000", rows.get(0).getValue("c"));
    Assert.assertEquals("3:4.200000", rows.get(1).getValue("c"));
    Assert.assertEquals(3, rows.get(0).width());
    Assert.assertEquals(3, rows.get(1).width());
  }

  @Test
  public void testHttpInvokeWithWrongEndPoint() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/wrongserviceendpoint ") + "a,b"
    };

    List<Row> rows = Arrays.asList(
      new Row("a", "1").add("b", 2.0),
      new Row("a", "3").add("b", 4.2)
    );


    RecipePipeline executor = TestingRig.execute(directives);
    rows = executor.execute(rows);
    Assert.assertTrue(rows.size() == 0);
    Assert.assertTrue(executor.errors().size() == 2);
  }

  @Test
  public void testHttpInvokeBlocksSsrfTargetsByDefault() throws Exception {
    System.clearProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY);
    List<Row> rows = Arrays.asList(
      new Row("a", "1").add("b", 2.0)
    );
    String[] blockedUrls = new String[] {
      baseURL + "/service",
      "http://127.0.0.1:8080/service",
      "http://169.254.169.254/computeMetadata/v1/",
      "http://metadata.google.internal/computeMetadata/v1/",
      "http://cdap-master.default.svc.cluster.local:11015/v3/namespaces",
      "http://10.0.0.1/internal",
      "file:///etc/passwd"
    };
    for (String target : blockedUrls) {
      try {
        TestingRig.execute(new String[] {"invoke-http " + target + " a,b"}, rows);
        Assert.fail("Expected invoke-http to reject target URL: " + target);
      } catch (RecipeException expected) {
        // expected
      }
    }

    System.setProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY, "true");
    String[] alwaysBlockedUrls = new String[] {
      "http://169.254.169.254/computeMetadata/v1/",
      "http://metadata.google.internal/computeMetadata/v1/",
      "http://cdap-master.default.svc.cluster.local:11015/v3/namespaces"
    };
    for (String target : alwaysBlockedUrls) {
      try {
        TestingRig.execute(new String[] {"invoke-http " + target + " a,b"}, rows);
        Assert.fail("Expected invoke-http to reject metadata/cluster URL even with opt-in: " + target);
      } catch (RecipeException expected) {
        // expected
      }
    }
  }

  @Test
  public void testHttpInvokeDoesNotFollowRedirects() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/redirect ") + "a,b"
    };
    List<Row> rows = Arrays.asList(
      new Row("a", "1").add("b", 2.0)
    );
    RecipePipeline executor = TestingRig.execute(directives);
    rows = executor.execute(rows);
    Assert.assertEquals(0, rows.size());
    Assert.assertEquals(1, executor.errors().size());
  }

  @After
  public void stopService() throws Exception {
    System.clearProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY);
    httpService.stop();
  }

}
