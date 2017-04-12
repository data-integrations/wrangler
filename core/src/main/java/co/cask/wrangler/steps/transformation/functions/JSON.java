/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation.functions;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
public final class JSON {
  public static final Configuration GSON_CONFIGURATION = Configuration
    .builder()
    .mappingProvider(new GsonMappingProvider())
    .jsonProvider(new GsonJsonProvider())
    .build();

  private static final JsonParser PARSER = new JsonParser();

  public static final JsonElement select(String json, String path, String ...paths) {
    JsonElement element = PARSER.parse(json);
    return select(element, path, paths);
  }

  public static final JsonElement select(String json, boolean toLower, String path, String ...paths) {
    JsonElement element = PARSER.parse(json);
    return select(element, toLower, path, paths);
  }

  public static final JsonElement select(JsonElement element, String path, String ...paths) {
    return select(element, false, path, paths);
  }

  public static final JsonElement select(JsonElement element, boolean toLower, String path, String ...paths) {
    if (toLower) {
      element = keysToLower(element);
    }
    DocumentContext context = JsonPath.using(GSON_CONFIGURATION).parse(element);
    if (paths.length == 0) {
      return context.read(path);
    } else {
      JsonArray array = new JsonArray();
      array.add((JsonElement)context.read(path));
      for (String p : paths) {
        array.add((JsonElement)context.read(p));
      }
      return array;
    }
  }

  public static final JsonElement drop(String json, String field, String ... fields) {
    JsonElement element = PARSER.parse(json);
    return drop(element, field, fields);
  }

  /**
   * Removes fields from a JSON inline.
   *
   * This method recursively iterates through the Json to delete one or more fields specified.
   * It requires the Json to be parsed.
   *
   * @param element Json element to be parsed.
   * @param field first field to be deleted.
   * @param fields list of fields to be deleted.
   * @return
   */
  public static final JsonElement drop(JsonElement element, String field, String ... fields) {
    if(element.isJsonObject()) {
      JsonObject object = element.getAsJsonObject();
      Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
      Iterator<Map.Entry<String, JsonElement>> iterator = entries.iterator();
      while(iterator.hasNext()) {
        Map.Entry<String, JsonElement> next = iterator.next();
        drop(next.getValue(), field, fields);
      }
      object.remove(field);
      for (String fld : fields) {
        object.remove(fld);
      }
    } else if (element.isJsonArray()) {
      JsonArray object = element.getAsJsonArray();
      for (int i = 0; i < object.size(); ++i) {
        JsonElement arrayElement = object.get(i);
        if (arrayElement.isJsonObject()) {
          drop(arrayElement, field, fields);
        }
      }
    }
    return element;
  }

  /**
   * This function lowers the keys of the json. it applies this transformation recurively.
   *
   * @param element to be transformed.
   * @return modified element.
   */
  public static final JsonElement keysToLower(JsonElement element) {
    if (element.isJsonObject()) {
      JsonObject newObject = new JsonObject();
      JsonObject object = element.getAsJsonObject();
      Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
      Iterator<Map.Entry<String, JsonElement>> iterator = entries.iterator();
      while(iterator.hasNext()) {
        Map.Entry<String, JsonElement> next = iterator.next();
        String name = next.getKey();
        JsonElement child = next.getValue();
        newObject.add(name.toLowerCase(), keysToLower(child));
      }
      return newObject;
    } else if (element.isJsonArray()) {
      JsonArray newArray = new JsonArray();
      JsonArray array = element.getAsJsonArray();
      for (int i = 0; i < array.size(); ++i) {
        newArray.add(keysToLower(array.get(i)));
      }
      return newArray;
    }
    return element;
  }

  public static String join(JsonElement element, String separator) {
    StringBuilder sb = new StringBuilder();
    if (element instanceof JsonArray) {
      JsonArray array = element.getAsJsonArray();
      for (int i = 0; i < array.size(); ++i) {
        JsonElement value = array.get(i);
        if (value == null) {
          continue;
        }
        if (value instanceof JsonPrimitive) {
          sb.append(value);
        }
        sb.append(separator);
      }
    }
    return sb.toString();
  }

  /**
   * This method converts a JavaScript value to a JSON string.
   *
   * @param element the value to convert to JSON string
   * @return a JSON string.
   */
  public static String stringify(JsonElement element) {
    if (element == null) {
      return "null";
    }
    return element.toString();
  }

  /**
   * Parses a column or string to JSON. This is equivalent to <code>JSON.parse()</code>
   * This function by default lowercases the keys.
   *
   * @param json string representation of json.
   * @return parsed json else throws an exception.
   */
  public static JsonElement parse(String json) {
    return parse(json, true);
  }

  /**
   * Parses a column or string to JSON. This is equivalent to <code>JSON.parse()</code>
   *
   * @param json string representation of json.
   * @param toLower true to lower case keys, false to leave it as-is.
   * @return parsed json else throws an exception.
   */
  public static JsonElement parse(String json, boolean toLower) {
    JsonElement element = PARSER.parse(json);
    if (toLower) {
      element = keysToLower(element);
    }
    return element;
  }
}

