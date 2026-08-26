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

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Custom GSON deserializer for {@link DirectiveConfig}.
 */
public final class DirectiveConfigDeserializer implements JsonDeserializer<DirectiveConfig> {
  private static final Type STRING_SET_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type JEXL_ALLOWLIST_LIST_TYPE = new TypeToken<List<JexlAllowlist>>() { }.getType();

  @Override
  public DirectiveConfig deserialize(JsonElement configJson, Type typeOfT, JsonDeserializationContext ctx)
      throws JsonParseException {
    JsonObject configJsonObj = configJson.getAsJsonObject();

    return new DirectiveConfig(
        deserializeProperty(configJsonObj, DirectiveConfig.EXCLUSIONS_KEY, STRING_SET_TYPE, ctx),
        deserializeProperty(configJsonObj, DirectiveConfig.ALIASES_KEY, STRING_MAP_TYPE, ctx),
        deserializeProperty(configJsonObj, DirectiveConfig.JEXL_ALLOWLIST_KEY, JEXL_ALLOWLIST_LIST_TYPE, ctx));
  }

  @Nullable
  private static <T> T deserializeProperty(JsonObject obj, String key, Type type, JsonDeserializationContext ctx) {
    JsonElement element = obj.get(key);
    return (element != null && !element.isJsonNull()) ? ctx.deserialize(element, type) : null;
  }
}
