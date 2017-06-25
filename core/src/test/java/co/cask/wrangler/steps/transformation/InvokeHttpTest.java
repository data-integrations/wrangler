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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.common.Bytes;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.SimpleTextParser;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Tests {@link InvokeHttp}
 */
public class InvokeHttpTest {
  private static NettyHttpService httpService;
  protected static String baseURL;

  @Before
  public void startService() throws Exception {
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new ServiceHandler());
    httpService = NettyHttpService.builder("Services")
      .addHttpHandlers(handlers)
      .build();
    httpService.startAsync().awaitRunning(10L, TimeUnit.SECONDS);
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
    public void append(HttpRequest request, HttpResponder responder) {
      Map<String, Object> object = postRequest(request);
      Map<String, Object> response = new HashMap<>();
      String c = String.format("%s:%f", object.get("a"), object.get("b"));
      String headerC = request.getHeader("C");
      response.put("c", c);
      response.put("HeaderC", headerC);
      responder.sendJson(HttpResponseStatus.OK, response);
    }

    private Map<String, Object> postRequest(HttpRequest request) throws JsonParseException {
      ByteBuffer content = request.getContent().toByteBuffer();
      if (content != null && content.hasRemaining()) {
        Map<String, Object> req =
          new Gson().fromJson(Bytes.toString(content), new TypeToken<Map<String, Object>>(){}.getType());
        return req;
      }
      return null;
    }
  }

  @Test
  public void testHttpInvokeWithHeaders() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/service ") + "a,b A=1,B=2,C=3"
    };

    List<Record> records = Arrays.asList(
      new Record("a", "1").add("b", 2.0),
      new Record("a", "3").add("b", 4.2)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);

    Assert.assertEquals(2, records.size());
    Assert.assertEquals(4, records.get(0).length());
    Assert.assertEquals(4, records.get(1).length());
    Assert.assertEquals("1:2.000000", records.get(0).getValue("c"));
    Assert.assertEquals("3:4.200000", records.get(1).getValue("c"));
    Assert.assertEquals("3", records.get(0).getValue("HeaderC"));
    Assert.assertEquals("3", records.get(1).getValue("HeaderC"));
  }

  @Test
  public void testHttpInvokeWithOutHeaders() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/service ") + "a,b"
    };

    List<Record> records = Arrays.asList(
      new Record("a", "1").add("b", 2.0),
      new Record("a", "3").add("b", 4.2)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);

    Assert.assertEquals(2, records.size());
    Assert.assertEquals("1:2.000000", records.get(0).getValue("c"));
    Assert.assertEquals("3:4.200000", records.get(1).getValue("c"));
    Assert.assertEquals(3, records.get(0).length());
    Assert.assertEquals(3, records.get(1).length());
  }

  @Test
  public void testHttpInvokeWithWrongEndPoint() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/wrongserviceendpoint ") + "a,b"
    };

    List<Record> records = Arrays.asList(
      new Record("a", "1").add("b", 2.0),
      new Record("a", "3").add("b", 4.2)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);

    Assert.assertTrue(records.size() == 0);
    Assert.assertTrue(pipeline.errors().size() == 2);
  }

  @After
  public void stopService() throws Exception {
    httpService.stopAsync().awaitTerminated(10L, TimeUnit.SECONDS);
  }

}