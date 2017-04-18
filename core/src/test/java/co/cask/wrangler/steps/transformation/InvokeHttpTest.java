package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.common.Bytes;
import co.cask.http.HandlerContext;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.PipelineTest;
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
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Class description here.
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
    httpService.startAsync().awaitRunning();
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
      response.put("c", c);
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
  public void testHttpInvoke() throws Exception {
    String[] directives = new String[] {
      "invoke-http " + (baseURL + "/service ") + "a,b"
    };

    List<Record> records = Arrays.asList(
      new Record("a", "1").add("b", 2.0),
      new Record("a", "3").add("b", 4.2)
    );

    records = PipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 2);
    Assert.assertEquals("c", records.get(0).getColumn(2));
  }

  @After
  public void stopService() throws Exception {
    httpService.stopAsync().awaitTerminated();
  }

}