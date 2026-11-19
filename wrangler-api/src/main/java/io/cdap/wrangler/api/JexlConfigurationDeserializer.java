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
import javax.annotation.Nullable;

/**
 * Custom GSON deserializer for {@link JexlConfiguration}.
 */
@PublicEvolving
public final class JexlConfigurationDeserializer implements JsonDeserializer<JexlConfiguration> {
  private static final Type JEXL_ALLOWLIST_LIST_TYPE = new TypeToken<List<JexlAllowlist>>() { }.getType();

  @Override
  public JexlConfiguration deserialize(JsonElement jexlConfigJson, Type typeOfT, JsonDeserializationContext ctx)
      throws JsonParseException {
    if (!jexlConfigJson.isJsonObject()) {
      throw new JsonParseException("Expected jexlConfiguration to be a JSON object.");
    }
    JsonObject jexlConfigJsonObj = jexlConfigJson.getAsJsonObject();

    return new JexlConfiguration(
        deserializeProperty(jexlConfigJsonObj, JexlConfiguration.JEXL_ALLOWLIST_ENABLED_KEY, boolean.class, true, ctx),
        deserializeProperty(
            jexlConfigJsonObj, JexlConfiguration.JEXL_ALLOWLIST_KEY, JEXL_ALLOWLIST_LIST_TYPE, null, ctx));
  }

  @Nullable
  private static <T> T deserializeProperty(
      JsonObject obj, String key, Type type, @Nullable T defaultValue, JsonDeserializationContext ctx) {
    JsonElement element = obj.get(key);
    return (element != null && !element.isJsonNull()) ? ctx.deserialize(element, type) : defaultValue;
  }
}
