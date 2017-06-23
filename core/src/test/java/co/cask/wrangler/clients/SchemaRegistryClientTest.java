package co.cask.wrangler.clients;

import co.cask.cdap.api.common.Bytes;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Tests {@link SchemaRegistryClient}
 *
 * NOTE: Due to issues with the Guava inclusion in the core, the {@link co.cask.cdap.test.TestBase} has
 * issue loading class. Hence, this test simulates the service and not actually tests the service.
 */
public class SchemaRegistryClientTest {
  private static NettyHttpService httpService;
  protected static String baseURL;
  private static SchemaRegistryClient client;

  @Before
  public void startService() throws Exception {
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new ServiceHandler());
    httpService = NettyHttpService.builder("SchemaService")
      .addHttpHandlers(handlers)
      .build();
    httpService.startAndWait();
    int port = httpService.getBindAddress().getPort();
    baseURL = "http://localhost:" + port;
    client = new SchemaRegistryClient(baseURL);
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
    @Path("schemas")
    public void append(HttpRequest request, HttpResponder responder) {
      OKResponse response = new OKResponse(200, "Successfully created schema entry with id '%s', name '%s'");
      responder.sendJson(HttpResponseStatus.OK, response);
    }

    @GET
    @Path("schemas/foo/versions/1")
    public void get(HttpRequest request, HttpResponder responder) {
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
      Iterator<Long> it = vs.iterator();
      while(it.hasNext()) {
        versions.add(new JsonPrimitive(it.next()));
      }
      object.add("versions", versions);
      array.add(object);
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      responder.sendJson(HttpResponseStatus.OK, response);
    }

    @GET
    @Path("schemas/foo/versions")
    public void getVersions(HttpRequest request, HttpResponder responder) {
      Set<Long> versions = new HashSet<>();
      versions.add(1L);
      versions.add(2L);
      versions.add(3L);
      versions.add(4L);
      JsonObject response = new JsonObject();
      JsonArray array = new JsonArray();
      Iterator<Long> it = versions.iterator();
      while(it.hasNext()) {
        array.add(new JsonPrimitive(it.next()));
      }
      response.addProperty("status", HttpURLConnection.HTTP_OK);
      response.addProperty("message", "Success");
      response.addProperty("count", array.size());
      response.add("values", array);
      responder.sendJson(HttpResponseStatus.OK, response);
    }
  }

  @Test
  public void testGetSchema() throws Exception {
    byte[] bytes = client.getSchema("foo", 1);
    Assert.assertNotNull(bytes);
    String str = Bytes.toString(bytes);
    Assert.assertEquals("{\"foo\" : \"test\"}", str);
  }

  @Test
  public void testGetVersions() throws Exception {
    List<Long> response = client.getVersions("foo");
    Assert.assertEquals(4, response.size());
    Assert.assertNotNull(response);
  }

  @Test (expected = RestClientException.class)
  public void testGetWrongSchemaIdVersions() throws Exception {
    client.getVersions("foo1");
  }

  @After
  public void stopService() throws Exception {
    httpService.stopAndWait();
  }
}
