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

package io.cdap.wrangler.service.directive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import io.cdap.wrangler.dataset.workspace.RequestDeserializer;
import io.cdap.wrangler.proto.Request;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests Request Parsing.
 */
public class RequestTest {

  @Test
  public void testDeserialization() throws Exception {
    String basic = "  {\n" +
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
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Request.class, new RequestDeserializer());
    Gson gson = builder.create();
    Request request = gson.fromJson(basic, Request.class);
    Assert.assertEquals("body", request.getWorkspace().getName());
    Assert.assertEquals(100, (int) request.getWorkspace().getResults());
  }

  @Test(expected = JsonParseException.class)
  public void testVersionMissing() throws Exception {
    String basic = "  {\n" +
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
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Request.class, new RequestDeserializer());
    Gson gson = builder.create();
    gson.fromJson(basic, Request.class);
  }

  @Test(expected = JsonParseException.class)
  public void testWithWrongVersion() throws Exception {
    String basic = "  {\n" +
      "    \"version\" : 2,\n" +
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
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Request.class, new RequestDeserializer());
    Gson gson = builder.create();
    gson.fromJson(basic, Request.class);
  }

  @Test
  public void testSaveAndNameNotPresent() throws Exception {
    String basic = "  {\n" +
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
      "      ]\n" +
      "    },\n" +
      "    \"sampling\" : {\n" +
      "      \"method\" : \"FIRST\",\n" +
      "      \"seed\" : 1,\n" +
      "      \"limit\" : 1000\n" +
      "    }\n" +
      "  }";
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Request.class, new RequestDeserializer());
    Gson gson = builder.create();
    Request request = gson.fromJson(basic, Request.class);
    Assert.assertNotNull(request);
    Assert.assertNull(request.getRecipe().getName());
    Assert.assertNull(request.getRecipe().getSave());
  }
}
