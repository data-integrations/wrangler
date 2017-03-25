package co.cask.wrangler.service;

import co.cask.wrangler.service.request.Request;
import co.cask.wrangler.service.request.RequestDeserializer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests RequestV1 Response
 */
public class RequestTest {

  private static String basic = "  {\n" +
    "    \"version\" : 1,\n" +
    "    \"workspace\" : {\n" +
    "      \"name\" : \"body\",\n" +
    "      \"results\" : 100\n" +
    "    },\n" +
    "    \"recipe\" : {\n" +
    "      \"directives\" : [\n" +
    "        \"parse-as-csv body ,\",\n" +
    "        \"drop body\",\n" +
    "        \"set-columns a,b,c,d\"\n" +
    "      ],\n" +
    "      \"save\" : true,\n" +
    "      \"name\" : \"my-recipe\"\n" +
    "    },\n" +
    "    \"sampling\" : {\n" +
    "      \"method\" : \"FIRST\",\n" +
    "      \"seed\" : 1,\n" +
    "      \"limit\" : 1000\n" +
    "    }\n" +
    "  }";

  @Test
  public void testDeserialization() throws Exception {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Request.class, new RequestDeserializer());
    Gson gson = builder.create();
    Request request = gson.fromJson(basic, Request.class);
    Assert.assertNotNull(request);
  }
}
