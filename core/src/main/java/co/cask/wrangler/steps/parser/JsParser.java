/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.dq.TypeInference;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.LazilyParsedNumber;
import org.json.JSONException;
import org.json.JSONObject;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A Json Parser Stage for parsing the {@link Record} provided based on configuration.
 */
@Usage(
  directive = "parse-as-json",
  usage = "parse-as-json <column> [depth]",
  description = "Parses a column as JSON."
)
public class JsParser extends AbstractStep {
  // Column within the input row that needs to be parsed as Json
  private String col;

  // Max depth to which the JSON needs to be parsed.
  private int maxDepth;

  // JSON parser.
  private static final JsonParser parser = new JsonParser();

  public JsParser(int lineno, String detail, String col, int maxDepth) {
    super(lineno, detail);
    this.col = col;
    this.maxDepth = maxDepth;
  }

  /**
   * Parses a give column in a {@link Record} as a CSV Record.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return New Row containing multiple columns based on CSV parsing.
   * @throws StepException In case CSV parsing generates more record.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    // Iterate through all the records.
    for (Record record : records) {
      int idx = record.find(col);

      // If the input column exists in the record, proceed further.
      if (idx != -1) {
        Object value = record.getValue(idx);

        if (value == null) {
          continue;
        }

        try {
          JsonElement element = null;
          if(value instanceof String) {
            String document = (String) value;
            element = parser.parse(document);
          } else if (value instanceof JsonObject || value instanceof JsonArray) {
            element = (JsonElement) value;
          } else {
            throw new StepException(
              String.format("%s : Invalid type '%s' of column '%s'. " +
                              "Should be of type string. Use paths to further parse data.",
                            toString(), element != null ? element.getClass().getName() : "null", col)
            );
          }

          record.remove(idx);

          if (element != null) {
            if (element instanceof JsonObject) {
              flattenJson(element.getAsJsonObject(), col, 1, maxDepth, record);
              results.add(record);
            } else if (element instanceof JsonArray) {
              JsonArray array = element.getAsJsonArray();
              for(int i = 0; i < array.size(); ++i) {
                JsonElement object = array.get(i);
                Record newRecord = new Record(record);
                newRecord.add(col, getValue(object));
                results.add(newRecord);
              }
            } else if (element instanceof JsonPrimitive) {
              record.add(col, getValue(element.getAsJsonPrimitive()));
            }
          }
        } catch (JSONException e) {
          throw new StepException(toString() + " : " + e.getMessage());
        }
      }
    }
    return results;
  }

  /**
   * Recursively flattens JSON until the 'maxDepth' is reached.
   *
   * @param root of the JSONObject
   * @param field name to be used to be stored in the record.
   * @param depth current depth into JSON structure.
   * @param maxDepth maximum depth to reach
   * @param record to which the flatten fields need to be added.
   */
  public static void flattenJson(JsonObject root, String field, int depth, int maxDepth, Record record) {
    if (depth > maxDepth) {
      record.addOrSet(String.format("%s", field), root);
      return;
    }

    Iterator<Map.Entry<String, JsonElement>> elements = root.entrySet().iterator();
    while(elements.hasNext()) {
      Map.Entry<String, JsonElement> next = elements.next();
      String key = next.getKey();
      JsonElement element = next.getValue();
      if (element instanceof JsonObject) {
        flattenJson(element.getAsJsonObject(),
                    String.format("%s_%s", field, key), depth++, maxDepth, record);
      } else {
        record.add(String.format("%s_%s", field, key), getValue(element));
      }
    }
  }

  /**
   * Gets a single value from the {@link JsonElement}. The value could be
   * {@link JsonObject} or {@link JsonArray} or {@link JsonPrimitive}.
   *
   * @param element value to be extracted.
   * @return the sub-element, else {@link com.google.gson.JsonNull}.
   */
  public static Object getValue(JsonElement element) {
    if (element.isJsonObject()) {
      return (JsonObject) element.getAsJsonObject();
    } else if (element.isJsonArray()) {
      return (JsonArray) element.getAsJsonArray();
    } else if (element.isJsonPrimitive()) {
      return getValue(element.getAsJsonPrimitive());
    }
    return element.getAsJsonNull();
  }

  /**
   * Extracts a value from the {@link JsonPrimitive}.
   *
   * @param primitive object to extract real value.
   * @return java type extracted from the {@link JsonPrimitive}
   */
  public static Object getValue(JsonPrimitive primitive) {
    if (primitive.isBoolean()) {
      return primitive.getAsBoolean();
    } else if (primitive.isNumber()) {
      Number number = primitive.getAsNumber();
      if (number instanceof Long) {
        return number.longValue();
      } else if (number instanceof Double) {
        return number.doubleValue();
      } else if (number instanceof Integer) {
        return number.intValue();
      } else if (number instanceof Short) {
        return number.shortValue();
      } else if (number instanceof Float) {
        return number.floatValue();
      } else if (number instanceof BigInteger) {
        return primitive.getAsBigInteger().longValue();
      } else if (number instanceof BigDecimal) {
        return primitive.getAsBigDecimal().doubleValue();
      } else if (number instanceof LazilyParsedNumber) {
        if (TypeInference.isInteger(primitive.getAsString())) {
          return primitive.getAsBigInteger().longValue();
        } else {
          return primitive.getAsBigDecimal().doubleValue();
        }
      }
    } else if (primitive.isString()) {
      return primitive.getAsString();
    } else if (primitive.isJsonNull()) {
      return primitive.getAsJsonNull();
    }
    return null;
  }

  /**
   * Converts a {@link JSONObject} to {@link JsonElement}. This is used when converting
   * XML to JSON.
   *
   * @param object {@link JSONObject} type to be converted.
   * @return converted type {@link JsonElement}
   */
  public static JsonElement convert(JSONObject object) {
    return new Gson().fromJson(object.toString(), JsonElement.class);
  }

}
