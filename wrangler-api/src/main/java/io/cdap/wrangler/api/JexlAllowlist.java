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

import com.google.common.base.Strings;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.lang.model.SourceVersion;

/**
 * Defines custom class, method, and property inclusion rules.
 */
@PublicEvolving
public final class JexlAllowlist {
  public static final String INCLUDE_ALL_WILDCARD = "*";

  /**
   * The fully qualified name of the class to include (e.g. java.lang.Math).
   */
  @Nonnull
  private final String className;

  /**
   * The list of allowed methods for the class.
   */
  private final List<String> methods;

  /**
   * The list of allowed properties for the class.
   */
  private final List<String> properties;

  /**
   * Custom GSON deserializer for {@link JexlAllowlist}.
   * Invokes the parameterized constructor to guarantee validation is executed.
   */
  public static final class JexlAllowlistDeserializer implements JsonDeserializer<JexlAllowlist> {
    @Override
    public JexlAllowlist deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
        throws JsonParseException {

      JsonObject jexlAllowlistJson = json.getAsJsonObject();

      JsonElement classElement = jexlAllowlistJson.get("className");
      String className = (classElement != null && classElement.isJsonPrimitive())
          ? classElement.getAsString()
          : "";

      List<String> methods = Collections.emptyList();
      if (jexlAllowlistJson.has("methods") && !jexlAllowlistJson.get("methods").isJsonNull()) {
        methods = context.deserialize(jexlAllowlistJson.get("methods"), new TypeToken<List<String>>() {
        }.getType());
      }

      List<String> properties = Collections.emptyList();
      if (jexlAllowlistJson.has("properties") && !jexlAllowlistJson.get("properties").isJsonNull()) {
        properties = context.deserialize(jexlAllowlistJson.get("properties"), new TypeToken<List<String>>() {
        }.getType());
      }

      return new JexlAllowlist(className, methods, properties);
    }
  }

  public JexlAllowlist(@Nonnull String className, List<String> methods, List<String> properties) {
    if (!isValidClassName(className)) {
      throw new IllegalArgumentException("className cannot be null, empty, or an invalid Java name: " + className);
    }
    this.className = className;
    this.methods = sanitizeList(methods, "method");
    this.properties = sanitizeList(properties, "property");
  }

  private static List<String> sanitizeList(List<String> list, String type) {
    if (list == null || list.isEmpty()) {
      throw new IllegalArgumentException("The " + type + " list cannot be null or empty.");
    }
    return Collections.unmodifiableList(list.stream()
        .map(item -> {
          if (!INCLUDE_ALL_WILDCARD.equals(item) && !isValidIdentifier(item)) {
            throw new IllegalArgumentException("Invalid " + type + " name: " + item);
          }
          return item;
        })
        .collect(Collectors.toList()));
  }

  private static boolean isValidClassName(String className) {
    if (Strings.isNullOrEmpty(className)) {
      return false;
    }
    return SourceVersion.isName(className);
  }

  private static boolean isValidIdentifier(String name) {
    if (Strings.isNullOrEmpty(name)) {
      return false;
    }
    return SourceVersion.isIdentifier(name);
  }

  /**
   * Gets the class name.
   *
   * @return the class name
   */
  @Nonnull
  public String getClassName() {
    return className;
  }

  /**
   * Gets the list of allowed methods.
   *
   * @return the allowed methods
   */
  public List<String> getMethods() {
    return methods;
  }

  /**
   * Gets the list of allowed properties.
   *
   * @return the allowed properties
   */
  public List<String> getProperties() {
    return properties;
  }

  /**
   * Checks if all methods are allowed.
   *
   * @return true if all methods are allowed
   */
  public boolean isAllMethods() {
    return methods.contains(INCLUDE_ALL_WILDCARD);
  }

  /**
   * Checks if all properties are allowed.
   *
   * @return true if all properties are allowed
   */
  public boolean isAllProperties() {
    return properties.contains(INCLUDE_ALL_WILDCARD);
  }
}
