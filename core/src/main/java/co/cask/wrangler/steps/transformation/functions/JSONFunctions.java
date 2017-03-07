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

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Collection of useful expression functions made available in the context
 * of an expression.
 *
 * set-column column <expression>
 */
public class JSONFunctions {

  /**
   * Joins the elements in the array with a separator to return a String object.
   *
   * @param array JSON Array to be joined.
   * @param separator between elements of the JSON Array.
   * @return Joined String of elements of JSON array.
   */
  public static String ARRAY_JOIN(JSONArray array, String separator) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < array.length(); ++i) {
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if ( !(value instanceof JSONObject) ) {
        sb.append(value);
      } else {
        break;
      }
      sb.append(separator);
    }
    return sb.toString();
  }

  /**
   * Computes SUM of elements of JSON Array.
   *
   * @param array to be summed.
   * @return sum.
   */
  public static Double ARRAY_SUM(JSONArray array) {
    double sum = 0.0;
    for (int i = 0; i < array.length(); ++i) {
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if (value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof Short) {
        double v = array.getDouble(i);
        sum = sum + v;
      } else {
        break;
      }
    }
    return sum;
  }

  /**
   * Determines the MAX element from the JSON Array.
   *
   * @param array to extract max.
   * @return max
   */
  public static Double ARRAY_MAX(JSONArray array) {
    double max = Double.MIN_VALUE;
    for (int i = 0; i < array.length(); ++i) {
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if (value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof Short) {
       double v = array.getDouble(i);
        if (max < v) {
          max = v;
        }
      } else {
        break;
      }
    }
    return max;
  }

  /**
   * Determines the MIN element from the JSON Array.
   *
   * @param array to extract min.
   * @return min
   */
  public static Double ARRAY_MIN(JSONArray array) {
    double min = Double.MAX_VALUE;
    for (int i = 0; i < array.length(); ++i) {
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if (value instanceof Integer || value instanceof Double || value instanceof Float || value instanceof Short) {
        double v = array.getDouble(i);
        if (min > v) {
          min = v;
        }
      } else {
        break;
      }
    }
    return min;
  }

  /**
   * Returns the length of the array.
   *
   * @param array JSON Array
   * @return length of array.
   */
  public static int ARRAY_LENGTH(JSONArray array) {
    if (array != null) {
      return array.length();
    }
    return 0;
  }

  /**
   * Drops fields from JSON.
   *
   * @param array to be modified.
   * @param fields list of fields to drop.
   * @return modified array.
   */
  public static JSONArray ARRAY_OBJECT_DROP_FIELDS(JSONArray array, String fields) {
    String[] cols = fields.split(",");
    Set<String> fieldSet = new HashSet<>();
    for (String col : cols) {
      fieldSet.add(col.trim());
    }

    JSONArray newarray = new JSONArray();
    // Iterate through each object in the array.
    for (int i = 0; i < array.length(); ++i) {
      JSONObject newobject = new JSONObject();
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if (value instanceof JSONObject) {
        JSONObject v = array.getJSONObject(i);
        Iterator<String> it = v.keys();
        while (it.hasNext()) {
          String name = it.next();
          if (fieldSet.contains(name)) {
            continue;
          }
          newobject.put(name, v.get(name));
        }
        newarray.put(newobject);
      } else {
        return array;
      }
    }
    return newarray;
  }

  /**
   * @return String of Array.
   */
  public static String TO_STRING(JSONArray array) {
    return array.toString();
  }

  /**
   * @return String of object.
   */
  public static String TO_STRING(JSONObject object) {
    return object.toString();
  }

  /**
   * Removes null fields.
   *
   * @param array to filter the fields.
   * @param columns list of columns to be checked for.
   * @return modified array.
   */
  public static JSONArray ARRAY_OBJECT_REMOVE_NULL_FIELDS(JSONArray array, String columns) {
    String[] cols = columns.split(",");
    Set<String> columnSet = new HashSet<>();
    for (String col : cols) {
      columnSet.add(col.trim());
    }

    JSONArray newarray = new JSONArray();
    // Iterate through each object in the array.
    for (int i = 0; i < array.length(); ++i) {
      JSONObject newobject = new JSONObject();
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if (value instanceof JSONObject) {
        JSONObject v = array.getJSONObject(i);
        Iterator<String> it = v.keys();
        while (it.hasNext()) {
          String name = it.next();
          if (columnSet.contains(name)) {
            Object element = v.get(name);
            if (element == null || element == JSONObject.NULL) {
              continue;
            } else if (element instanceof String) {
              String strvalue = v.getString(name);
              if (strvalue.equalsIgnoreCase("null")) {
                continue;
              }
            }
            newobject.put(name, element);
          } else {
            newobject.put(name, v.get(name));
          }
        }
        newarray.put(newobject);
      } else {
        return array;
      }
    }
    return newarray;
  }

  /**
   * Rename fields in JSON.
   *
   * @param array of objects who's fields need to be renamed.
   * @param rename from:to[,from:to]
   * @return modified array.
   */
  public static JSONArray ARRAY_OBJECT_RENAME_FIELDS(JSONArray array, String rename) {
    String[] cols = rename.split(",");
    Map<String, String> columnSet = new HashMap<>();
    for (String col : cols) {
      col = col.trim();
      String[] splits = col.split(":");
      if (splits.length != 2) {
        return array;
      }
      String from = splits[0];
      String to = splits[1];
      columnSet.put(from, to);
    }

    JSONArray newarray = new JSONArray();
    // Iterate through each object in the array.
    for (int i = 0; i < array.length(); ++i) {
      JSONObject newobject = new JSONObject();
      Object value = array.get(i);
      if (value == null || JSONObject.NULL.equals(value)) {
        continue;
      }
      if (value instanceof JSONObject) {
        JSONObject v = array.getJSONObject(i);
        Iterator<String> it = v.keys();
        while (it.hasNext()) {
          String name = it.next();
          if (columnSet.containsKey(name)) {
            Object element = v.get(name);
            newobject.put(columnSet.get(name), element);
          } else {
            newobject.put(name, v.get(name));
          }
        }
        newarray.put(newobject);
      } else {
        return array;
      }
    }
    return newarray;
  }


}
