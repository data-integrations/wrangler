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

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Custom GSON deserializer for {@link JexlAllowlist}.
 * Invokes the parameterized constructor to guarantee validation is executed.
 */
@PublicEvolving
public final class JexlAllowlistDeserializer implements JsonDeserializer<JexlAllowlist> {

  private static final String CLASS_NAME_KEY = "className";
  private static final String METHODS_KEY = "methods";
  private static final String PROPERTIES_KEY = "properties";

  private static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() { }.getType();

  @Override
  public JexlAllowlist deserialize(JsonElement allowlistJson, Type typeOfT, JsonDeserializationContext ctx)
      throws JsonParseException {
    JsonObject allowlistJsonObject = allowlistJson.getAsJsonObject();

    try {
      return new JexlAllowlist(
          deserializeProperty(allowlistJsonObject, CLASS_NAME_KEY, String.class, "", ctx),
          deserializeProperty(allowlistJsonObject, METHODS_KEY, STRING_LIST_TYPE, null, ctx),
          deserializeProperty(allowlistJsonObject, PROPERTIES_KEY, STRING_LIST_TYPE, null, ctx));
    } catch (IllegalArgumentException e) {
      throw new JsonParseException(e.getMessage(), e);
    }
  }

  private static <T> T deserializeProperty(
      JsonObject obj, String key, Type type, T defaultValue, JsonDeserializationContext ctx) {
    JsonElement element = obj.get(key);
    if (element == null || element.isJsonNull()) {
      return defaultValue;
    }
    T result = ctx.deserialize(element, type);
    return result != null ? result : defaultValue;
  }
}
