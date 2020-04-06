/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.functions;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
public final class JsonFunctions {
  public static final Configuration GSON_CONFIGURATION = Configuration
    .builder()
    .mappingProvider(new GsonMappingProvider())
    .jsonProvider(new GsonJsonProvider())
    .options(Option.SUPPRESS_EXCEPTIONS)
    .build();

  private static final JsonParser PARSER = new JsonParser();
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  /**
   * Don't let anyone instantiate this class.
   */
  private JsonFunctions() {}

  /**
   * Parses a column or string to JSON. This is equivalent to <code>JSON.parse()</code>
   *
   * @param json string representation of json.
   * @return parsed json else throws an exception.
   */
  public static JsonElement Parse(String json) {
    try {
      JsonElement element = PARSER.parse(json);
      return element;
    } catch (JsonSyntaxException e) {
      return JsonNull.INSTANCE;
    }
  }

  /**
   * Checks if a json is valid.
   *
   * @param json to checked for validity.
   * @return true if valid, false otherwise.
   */
  public static boolean IsValid(String json) {
    try {
      JsonElement element = PARSER.parse(json);
      return true;
    } catch (JsonSyntaxException e) {
      return false;
    }
  }

  /**
   * Checks if a Json is {@code JsonNull}.
   *
   * @param element to be inspected for null.
   * @return true if null, false otherwise.
   */
  public static boolean IsNull(JsonElement element) {
    if (element != null && element.isJsonNull()) {
      return true;
    }
    return false;
  }

  /**
   * Checks if a Json is {@code JsonObject}.
   *
   * @param element to be inspected for object.
   * @return true if object, false otherwise.
   */
  public static boolean IsObject(JsonElement element) {
    if (element != null && element.isJsonObject()) {
      return true;
    }
    return false;
  }

  /**
   * Checks if a Json is {@code JsonArray}.
   *
   * @param element to be inspected for array.
   * @return true if array, false otherwise.
   */
  public static boolean IsArray(JsonElement element) {
    if (element != null && element.isJsonArray()) {
      return true;
    }
    return false;
  }

  /**
   * Selects part of JSON using JSON DSL specified as json path.
   *
   * @param element json to be inspected.
   * @param path to be searched for in the element.
   * @param paths other paths.
   * @return A json element containing the results of all json paths.
   */
  public static JsonElement Select(JsonElement element, String path, String ...paths) {
    DocumentContext context = JsonPath.using(GSON_CONFIGURATION).parse(element);
    if (paths.length == 0) {
      return context.read(path);
    } else {
      JsonArray array = new JsonArray();
      array.add((JsonElement) context.read(path));
      for (String p : paths) {
        array.add((JsonElement) context.read(p));
      }
      return array;
    }
  }

  /**
   * This method converts a JavaScript value to a JSON string.
   *
   * @param element the value to convert to JSON string
   * @return a JSON string.
   */
  public static String Stringify(JsonElement element) {
    if (element == null) {
      return GSON.toJson(JsonNull.INSTANCE);
    }
    return GSON.toJson(element);
  }

  /**
   * @return Number of elements in the array.
   */
  public static int ArrayLength(JsonArray array) {
    if (array != null) {
      return array.size();
    }
    return 0;
  }
}

