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

/**
 * Tests for {@link JexlAllowlistDeserializer}.
 */
public class JexlAllowlistDeserializerTest {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(JexlAllowlist.class, new JexlAllowlistDeserializer())
      .create();

  @Test
  public void testJexlAllowlistDeserialization() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"methods\": [\"max\", \"min\"],\n"
        + "  \"properties\": [\"*\"]\n"
        + "}";

    JexlAllowlist allowlist = GSON.fromJson(json, JexlAllowlist.class);
    Assert.assertNotNull(allowlist);
    Assert.assertEquals("java.lang.Math", allowlist.getClassName());
    Assert.assertEquals(Arrays.asList("max", "min"), allowlist.getMethods());
    Assert.assertEquals(Arrays.asList("*"), allowlist.getProperties());
    Assert.assertFalse(allowlist.allowAllMethods());
    Assert.assertTrue(allowlist.allowAllProperties());
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeMissingClassName() {
    String json = "{\n"
        + "  \"methods\": [\"*\"],\n"
        + "  \"properties\": [\"*\"]\n"
        + "}";
    GSON.fromJson(json, JexlAllowlist.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeInvalidClassName() {
    String json = "{\n"
        + "  \"className\": \"123InvalidClass\",\n"
        + "  \"methods\": [\"*\"],\n"
        + "  \"properties\": [\"*\"]\n"
        + "}";
    GSON.fromJson(json, JexlAllowlist.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeMissingMethods() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"properties\": [\"*\"]\n"
        + "}";
    GSON.fromJson(json, JexlAllowlist.class);
  }

  @Test
  public void testDeserializeEmptyMethods() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"methods\": [],\n"
        + "  \"properties\": [\"*\"]\n"
        + "}";
    JexlAllowlist allowlist = GSON.fromJson(json, JexlAllowlist.class);
    Assert.assertNotNull(allowlist);
    Assert.assertEquals("java.lang.Math", allowlist.getClassName());
    Assert.assertTrue(allowlist.getMethods().isEmpty());
    Assert.assertTrue(allowlist.blockAllMethods());
    Assert.assertFalse(allowlist.allowAllMethods());
    Assert.assertFalse(allowlist.blockAllProperties());
    Assert.assertTrue(allowlist.allowAllProperties());
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeMissingProperties() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"methods\": [\"*\"]\n"
        + "}";
    GSON.fromJson(json, JexlAllowlist.class);
  }

  @Test
  public void testDeserializeEmptyProperties() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"methods\": [\"*\"],\n"
        + "  \"properties\": []\n"
        + "}";
    JexlAllowlist allowlist = GSON.fromJson(json, JexlAllowlist.class);
    Assert.assertNotNull(allowlist);
    Assert.assertEquals("java.lang.Math", allowlist.getClassName());
    Assert.assertTrue(allowlist.getProperties().isEmpty());
    Assert.assertTrue(allowlist.blockAllProperties());
    Assert.assertFalse(allowlist.allowAllProperties());
    Assert.assertFalse(allowlist.blockAllMethods());
    Assert.assertTrue(allowlist.allowAllMethods());
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeBothEmptyMethodsAndProperties() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"methods\": [],\n"
        + "  \"properties\": []\n"
        + "}";
    GSON.fromJson(json, JexlAllowlist.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeInvalidMethodName() {
    String json = "{\n"
        + "  \"className\": \"java.lang.Math\",\n"
        + "  \"methods\": [\"invalid method!\"],\n"
        + "  \"properties\": [\"*\"]\n"
        + "}";
    GSON.fromJson(json, JexlAllowlist.class);
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeNonObjectJson() {
    String json = "[\"java.lang.Math\"]";
    GSON.fromJson(json, JexlAllowlist.class);
  }
}
