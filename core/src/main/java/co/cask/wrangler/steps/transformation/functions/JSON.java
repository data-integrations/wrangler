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

  private static final JsonParser parser = new JsonParser();

  public static final JsonElement select(String json, String path, String ...paths) {
    JsonElement element = parser.parse(json);
    return select(element, path, paths);
  }
  
  public static final JsonElement select(JsonElement element, String path, String ...paths) {
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

  public static final JsonElement remove(String json, String field, String ... fields) {
    JsonElement element = parser.parse(json);
    return remove(element, field, fields);
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
  public static final JsonElement remove(JsonElement element, String field, String ... fields) {
    if(element.isJsonObject()) {
      JsonObject object = element.getAsJsonObject();
      Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
      Iterator<Map.Entry<String, JsonElement>> iterator = entries.iterator();
      while(iterator.hasNext()) {
        Map.Entry<String, JsonElement> next = iterator.next();
        remove(next.getValue(), field, fields);
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
          remove(arrayElement, field, fields);
        }
      }
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
}

