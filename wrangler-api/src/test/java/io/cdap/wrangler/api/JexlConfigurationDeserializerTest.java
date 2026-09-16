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

/**
 * Tests for {@link JexlConfigurationDeserializer}.
 */
public class JexlConfigurationDeserializerTest {
  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(JexlAllowlist.class, new JexlAllowlistDeserializer())
      .registerTypeAdapter(JexlConfiguration.class, new JexlConfigurationDeserializer())
      .create();

  @Test
  public void testDeserializeValidConfiguration() {
    String json = "{\n"
        + "  \"jexlAllowlistEnabled\": true,\n"
        + "  \"jexlAllowlist\": [\n"
        + "    {\n"
        + "      \"className\": \"java.lang.Math\",\n"
        + "      \"methods\": [\"*\"],\n"
        + "      \"properties\": [\"*\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}";

    JexlConfiguration config = GSON.fromJson(json, JexlConfiguration.class);
    Assert.assertNotNull(config);
    Assert.assertTrue(config.isJexlAllowlistEnabled());
    Assert.assertNotNull(config.getJexlAllowlist());
    Assert.assertEquals(1, config.getJexlAllowlist().size());
    Assert.assertEquals("java.lang.Math", config.getJexlAllowlist().get(0).getClassName());
  }

  @Test
  public void testDeserializeEmptyObjectDefaultsToTrue() {
    JexlConfiguration config = GSON.fromJson("{}", JexlConfiguration.class);
    Assert.assertNotNull(config);
    Assert.assertTrue(config.isJexlAllowlistEnabled());
    Assert.assertNull(config.getJexlAllowlist());
  }

  @Test
  public void testDeserializeNullJexlAllowlistEnabledDefaultsToTrue() {
    JexlConfiguration config = GSON.fromJson("{\"jexlAllowlistEnabled\": null}", JexlConfiguration.class);
    Assert.assertNotNull(config);
    Assert.assertTrue(config.isJexlAllowlistEnabled());
  }

  @Test
  public void testDeserializeExplicitFalseEnabled() {
    JexlConfiguration config = GSON.fromJson("{\"jexlAllowlistEnabled\": false}", JexlConfiguration.class);
    Assert.assertNotNull(config);
    Assert.assertFalse(config.isJexlAllowlistEnabled());
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeNonObjectJsonThrowsJsonParseException() {
    GSON.fromJson("\"not-an-object\"", JexlConfiguration.class);
  }
}
