/*
 *  Copyright © 2020 Cask Data, Inc.
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
package io.cdap.wrangler.datamodel;

import io.cdap.http.HandlerContext;
import io.cdap.http.HttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.Schema;
import org.apache.commons.collections4.SetValuedMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Tests {@link HTTPSchemaLoader}
 */
public class HTTPSchemaLoaderTest {
  private static final String DATA_MODEL_NAME = "google.com.datamodels.TEST_DATA_MODEL";
  private static NettyHttpService httpService;

  @Before
  public void startService() throws Exception {
    List<HttpHandler> handlers = new ArrayList<>();
    handlers.add(new HTTPSchemaLoaderTest.ServiceHandler());
    httpService = NettyHttpService.builder("datamodel-bucket")
      .setHttpHandlers(handlers)
      .build();
    httpService.start();
  }

  @After
  public void stopService() throws Exception {
    httpService.stop();
  }

  public static class ServiceHandler implements HttpHandler {
    private static final String MANIFEST_JSON = ""
      + "{\n"
      + "  \"standards\": {\n"
      + "    \"TEST_DATA_MODEL\": {\n"
      + "      \"format\": \"avsc\",\n"
      + "      \"hash\": \"d8c119c43873ba2571c72ecccde8bb8c7b237746b227daf3ba78caf85feaca57\"\n"
      + "    }\n"
      + "  }\n"
      + "}";

    private static final String DATA_MODEL_RESOURCE = "{\n"
      + "    \"type\": \"record\",\n"
      + "    \"name\": \"TEST_DATA_MODEL\",\n"
      + "    \"namespace\": \"google.com.datamodels\",\n"
      + "    \"_revision\": \"1\",    \n"
      + "    \"fields\": [\n"
      + "        {\n"
      + "            \"name\": \"TEST_MODEL\",\n"
      + "            \"type\": [\n"
      + "                \"null\", {\n"
      + "                \"type\": \"record\",\n"
      + "                \"name\": \"TEST_MODEL\",\n"
      + "                \"namespace\": \"google.com.datamodels.Model\",\n"
      + "                \"fields\": [\n"
      + "                    {\n"
      + "                        \"name\": \"int_field\",\n"
      + "                        \"type\": [\"int\"]\n"
      + "                    }\n"
      + "                ]}\n"
      + "            ]\n"
      + "        }\n"
      + "    ]\n"
      + "}";

    @Override
    public void init(HandlerContext handlerContext) {
      // no-op
    }

    @Override
    public void destroy(HandlerContext handlerContext) {
      // no-op
    }

    @GET
    @Path("test-bucket/{resource}")
    public void get(HttpRequest request, HttpResponder responder, @PathParam("resource") String resource) {
      if (resource.equals("manifest.json")) {
        responder.sendJson(HttpResponseStatus.OK, MANIFEST_JSON);
      } else if (resource.equals("TEST_DATA_MODEL.avsc")) {
        responder.sendJson(HttpResponseStatus.OK, DATA_MODEL_RESOURCE);
      }
    }

    @GET
    @Path("errors/{resource}")
    public void errors(HttpRequest request, HttpResponder responder, @PathParam("resource") String resource) {
      String manifest = null;
      String dataModel = null;
      switch (resource) {
        case "missing_manifest.json":
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
          break;
        case "empty_manifest.json":
          responder.sendJson(HttpResponseStatus.OK, "{}");
          break;
        case "manifest_standard_mismatch.json":
          manifest = ""
            + "{\n"
            + "  \"standards\": {\n"
            + "    \"TEST_DATA_MODEL\": {\n"
            + "      \"format\": \"json\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
          responder.sendJson(HttpResponseStatus.OK, manifest);
          break;
        case "manifest_invalid_data_model.json":
          manifest = ""
            + "{\n"
            + "  \"standards\": {\n"
            + "    \"INVALID_TEST_DATA_MODEL\": {\n"
            + "      \"format\": \"avsc\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
          responder.sendJson(HttpResponseStatus.OK, manifest);
          break;
        case "INVALID_TEST_DATA_MODEL.avsc":
          dataModel = "{\n"
            + "    \"type\": \"__INVALID_TYPE__\",\n"
            + "    \"name\": \"TEST_DATA_MODEL\",\n"
            + "    \"_revision\": \"1\",    \n"
            + "    \"fields\": []\n"
            + "}";
          responder.sendJson(HttpResponseStatus.OK, dataModel);
          break;
      }
    }
  }

  @Test
  public void testLoad_dataModels_successfulLoad() throws Exception {
    String base = String.format("http://localhost:%d/test-bucket", httpService.getBindAddress().getPort());
    HTTPSchemaLoader client = new HTTPSchemaLoader(base, "manifest.json");
    SetValuedMap<String, Schema> glossary = client.load();

    Assert.assertNotNull(glossary);
    Assert.assertEquals(1, glossary.size());
    Set<Schema> dataModels = glossary.get(DATA_MODEL_NAME);
    Assert.assertEquals(1, dataModels.size());

    Schema dataModel = (Schema) dataModels.toArray()[0];
    Assert.assertEquals(DATA_MODEL_NAME, dataModel.getFullName());
    Assert.assertEquals(1, dataModel.getFields().size());
    Assert.assertNotNull(dataModel.getField("TEST_MODEL"));

    Schema model = dataModel.getField("TEST_MODEL").schema();
    Assert.assertNotNull(model);

    Schema modelType = model.getTypes().stream()
      .filter(t -> t.getType() == Schema.Type.RECORD)
      .findFirst()
      .orElse(null);
    Assert.assertNotNull(modelType);
    Assert.assertEquals("TEST_MODEL", modelType.getName());
    Assert.assertEquals(1, modelType.getFields().size());
    Assert.assertNotNull(modelType.getField("int_field"));
  }

  @Test(expected = IOException.class)
  public void testLoad_dataModels_manifestNotFound() throws Exception {
    String base = String.format("http://localhost:%d/errors", httpService.getBindAddress().getPort());
    HTTPSchemaLoader client = new HTTPSchemaLoader(base, "missing_manifest.json");
    client.load();
  }

  @Test
  public void testLoad_dataModels_emptyManifest() throws Exception {
    String base = String.format("http://localhost:%d/errors", httpService.getBindAddress().getPort());
    HTTPSchemaLoader client = new HTTPSchemaLoader(base, "empty_manifest.json");
    SetValuedMap<String, Schema> glossary = client.load();
    Assert.assertTrue(glossary.isEmpty());
  }

  @Test
  public void testLoad_dataModels_emptyStandards() throws Exception {
    String base = String.format("http://localhost:%d/errors", httpService.getBindAddress().getPort());
    HTTPSchemaLoader client = new HTTPSchemaLoader(base, "manifest_standard_mismatch.json");
    SetValuedMap<String, Schema> glossary = client.load();
    Assert.assertTrue(glossary.isEmpty());
  }

  @Test
  public void testLoad_dataModels_parserError() throws Exception {
    String base = String.format("http://localhost:%d/errors", httpService.getBindAddress().getPort());
    HTTPSchemaLoader client = new HTTPSchemaLoader(base, "manifest_invalid_data_model.json");
    SetValuedMap<String, Schema> glossary = client.load();
    Assert.assertTrue(glossary.isEmpty());
  }
}

