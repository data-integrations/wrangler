/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A class for generating json paths for a given json. It generates json path by recursively traversing
 * through the json structure.
 */
public final class JsonPathGenerator {
  // String representation of json
  private final JsonParser parser;

  public JsonPathGenerator() {
    this.parser = new JsonParser();
  }

  /**
   * Generates all json paths for the json being passed as a string.
   *
   * @param json string for which json paths need to be extracted.
   * @return list of json paths.
   */
  public List<String> get(String json) {
    if (json == null || json.isEmpty()) {
      return new ArrayList<>();
    }
    JsonElement object = parser.parse(json);
    return get(object);
  }

  /**
   * Generates all json paths for the json being passed as {@link JsonElement}.
   *
   * @param object {@link JsonElement} for the json
   * @return list of json paths.
   */
  public List<String> get(JsonElement object) {
    List<String> paths = new ArrayList<>();
    String root = "$";
    if (object.isJsonObject()) {
      paths.addAll(readObject(object.getAsJsonObject(), root));
    } else if (object.isJsonArray()) {
      paths.addAll(readArray(object.getAsJsonArray(), root));
    }
    return paths;
  }

  /**
   * Iterates through each key of the object, and depending on the type of the
   * object -- either it being {@link JsonArray} or {@link JsonObject}, it processes
   * them recursively.
   */
  private List<String> readObject(JsonObject object, String parent) {
    List<String> paths = new ArrayList<>();
    Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
    Iterator<Map.Entry<String, JsonElement>> iterator = entries.iterator();
    paths.add(parent);
    while (iterator.hasNext()) {
      Map.Entry<String, JsonElement> next = iterator.next();
      String base = parent + "." + next.getKey();
      JsonElement element = next.getValue();
      if (element.isJsonObject()) {
        paths.addAll(readObject(element.getAsJsonObject(), base));
      } else if (element.isJsonArray()) {
        paths.addAll(readArray(element.getAsJsonArray(), base));
      } else {
        paths.add(base);
      }
    }
    return paths;
  }

  private List<String> readArray(JsonArray array, String path) {
    List<String> paths = new ArrayList<>();
    paths.add(path);
    String parent = path;
    for (int i = 0; i < array.size(); ++i) {
      JsonElement value = array.get(i);
      path = parent + "[" + i + "]";
      if (value.isJsonArray()) {
        paths.addAll(readArray(value.getAsJsonArray(), path));
      } else if (value.isJsonObject()) {
        paths.addAll(readObject(value.getAsJsonObject(), path));
      } else {
        paths.add(path);
      }
    }
    return paths;
  }
}
