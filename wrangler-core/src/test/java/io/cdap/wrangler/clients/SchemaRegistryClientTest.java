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

package io.cdap.wrangler.clients;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.http.HandlerContext;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Tests {@link SchemaRegistryClient}
 *
 * NOTE: Due to issues with the Guava inclusion in the core, the {@link io.cdap.cdap.test.TestBase} has
 * issue loading class. Hence, this test simulates the service and not actually tests the service.
 */
public class SchemaRegistryClientTest {
  private static final Gson GSON = new Gson();
  private static NettyHttpService httpService;
  private static String baseURL;
  private static SchemaRegistryClient client;

  @Before
  public void startService() throws Exception {
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new ServiceHandler());
    httpService = NettyHttpService.builder("SchemaService")
      .setHttpHandlers(handlers)
      .build();
    httpService.start();
    int port = httpService.getBindAddress().getPort();
    baseURL = "http://localhost:" + port;
    client = new SchemaRegistryClient(baseURL);
  }

  @After
  public void stopService() throws Exception {
    httpService.stop();
  }

  public static class OKResponse {
    public OKResponse(int status, String message) {
      this.status = status;
      this.message = message;
    }
    private String message;

    public int getStatus() {
      return status;
    }

    private int status;

    public String getMessage() {
      return message;
    }
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

    @PUT
    @Path("contexts/{context}/schemas")
    public void append(HttpRequest request, HttpResponder responder, @PathParam("context") String context) {
      OKResponse response = new OKResponse(200, "Successfully created schema entry with id '%s', name '%s'");
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    }

    @GET
    @Path("contexts/{context}/schemas/foo/versions/1")
    public void get(HttpRequest request, HttpResponder responder, @PathParam("context") String context) {
      JsonObject response = new JsonObject();
      JsonArray array = new JsonArray();
      JsonObject object = new JsonObject();
      object.addProperty("id", "foo");
      object.addProperty("name", "Foo Name");
      object.addProperty("version", 1);
      object.addProperty("description", "Foo Description");
      object.addProperty("type", "avro");
      object.addProperty("current", 1);
      object.addProperty("specification", Bytes.toHexString("{\"foo\" : \"test\"}".getBytes(StandardCharsets.UTF_8)));
      JsonArray versions = new JsonArray();
      Set<Long> vs = new HashSet<>();
      vs.add(2L);
      vs.add(3L);
      for (Long v : vs) {
        versions.add(new JsonPrimitive(v));
      }
      object.add("versions", versions);
      array.add(object);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    }

    @GET
    @Path("contexts/{context}/schemas/foo/versions")
    public void getVersions(HttpRequest request, HttpResponder responder, @PathParam("context") String context) {
      Set<Long> versions = new HashSet<>();
      versions.add(1L);
      versions.add(2L);
      versions.add(3L);
      versions.add(4L);
      JsonObject response = new JsonObject();
      JsonArray array = new JsonArray();
      for (Long version : versions) {
        array.add(new JsonPrimitive(version));
      }
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(response));
    }
  }

  @Test
  public void testGetSchema() throws Exception {
    byte[] bytes = client.getSchema("c0", "foo", 1);
    Assert.assertNotNull(bytes);
    String str = Bytes.toString(bytes);
    Assert.assertEquals("{\"foo\" : \"test\"}", str);
  }

  @Test
  public void testGetVersions() throws Exception {
    List<Long> response = client.getVersions("c0", "foo");
    Assert.assertEquals(4, response.size());
    Assert.assertNotNull(response);
  }

  @Test (expected = RestClientException.class)
  public void testGetWrongSchemaIdVersions() throws Exception {
    client.getVersions("c0", "foo1");
  }
}
