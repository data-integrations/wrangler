/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link DirectiveConfigDeserializer}.
 */
public class DirectiveConfigDeserializerTest {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(DirectiveConfig.class, new DirectiveConfigDeserializer())
      .registerTypeAdapter(JexlAllowlist.class, new JexlAllowlistDeserializer())
      .create();

  @Test
  public void testDirectiveConfigDeserialization() {
    String json = "{\n"
        + "  \"exclusions\": [\"drop\"],\n"
        + "  \"aliases\": {\"p\": \"parse-as-json\"},\n"
        + "  \"jexlAllowlist\": [\n"
        + "    {\n"
        + "      \"className\": \"java.lang.String\",\n"
        + "      \"methods\": [\"*\"],\n"
        + "      \"properties\": [\"*\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    DirectiveConfig config = GSON.fromJson(json, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertTrue(config.isExcluded("drop"));
    Assert.assertEquals("parse-as-json", config.getAliasName("p"));
    Assert.assertEquals(Collections.singleton("drop"), config.getExclusions());
    Assert.assertEquals(Collections.singletonMap("p", "parse-as-json"), config.getAliases());
    Assert.assertNotNull(config.getJexlAllowlist());
    Assert.assertEquals(1, config.getJexlAllowlist().size());

    JexlAllowlist allowlist = config.getJexlAllowlist().get(0);
    Assert.assertEquals("java.lang.String", allowlist.getClassName());
    Assert.assertTrue(allowlist.allowAllMethods());
    Assert.assertTrue(allowlist.allowAllProperties());
  }

  @Test
  public void testDirectiveConfigDeserializationWithSpecificMethodsAndProperties() {
    String json = "{\n"
        + "  \"exclusions\": [\"drop\"],\n"
        + "  \"aliases\": {\"p\": \"parse-as-json\"},\n"
        + "  \"jexlAllowlist\": [\n"
        + "    {\n"
        + "      \"className\": \"java.lang.String\",\n"
        + "      \"methods\": [\"trim\", \"substring\"],\n"
        + "      \"properties\": [\"bytes\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    DirectiveConfig config = GSON.fromJson(json, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertTrue(config.isExcluded("drop"));
    Assert.assertEquals("parse-as-json", config.getAliasName("p"));
    Assert.assertEquals(Collections.singleton("drop"), config.getExclusions());
    Assert.assertEquals(Collections.singletonMap("p", "parse-as-json"), config.getAliases());
    Assert.assertNotNull(config.getJexlAllowlist());
    Assert.assertEquals(1, config.getJexlAllowlist().size());

    JexlAllowlist allowlist = config.getJexlAllowlist().get(0);
    Assert.assertEquals("java.lang.String", allowlist.getClassName());
    Assert.assertEquals(Arrays.asList("trim", "substring"), allowlist.getMethods());
    Assert.assertEquals(Arrays.asList("bytes"), allowlist.getProperties());
    Assert.assertFalse(allowlist.allowAllMethods());
    Assert.assertFalse(allowlist.allowAllProperties());
  }

  @Test
  public void testDirectiveConfigDeserializationEmpty() {
    String json = "{}";

    DirectiveConfig config = GSON.fromJson(json, DirectiveConfig.class);
    Assert.assertNotNull(config);
    Assert.assertTrue(config.getExclusions().isEmpty());
    Assert.assertTrue(config.getAliases().isEmpty());
    Assert.assertNull(config.getJexlAllowlist());
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeInvalidJexlAllowlistEntry() {
    String json = "{\n"
        + "  \"jexlAllowlist\": [\n"
        + "    {\n"
        + "      \"className\": \"123InvalidClass\",\n"
        + "      \"methods\": [\"*\"],\n"
        + "      \"properties\": [\"*\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}";
    GSON.fromJson(json, DirectiveConfig.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeMalformedExclusions() {
    String json = "{\n"
        + "  \"exclusions\": \"not-a-list\"\n"
        + "}";
    GSON.fromJson(json, DirectiveConfig.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeMalformedAliases() {
    String json = "{\n"
        + "  \"aliases\": \"not-a-map\"\n"
        + "}";
    GSON.fromJson(json, DirectiveConfig.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeNonObjectJson() {
    String json = "[\"drop\"]";
    GSON.fromJson(json, DirectiveConfig.class);
  }
}
