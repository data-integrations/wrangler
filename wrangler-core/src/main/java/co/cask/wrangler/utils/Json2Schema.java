/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

package co.cask.wrangler.utils;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.AbstractSchemaGenerator;
import co.cask.directives.parser.JsParser;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.Row;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.json.JSONException;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Json to Schema translates a JSON string into a {@link Schema} schema.
 *
 * This class takes in JSON as a string or parsed JSON as {@link JsonElement} and converts it to the
 * {@link Schema}. It's recursive in nature and it creates multi-level of {@link Schema}.
 */
public final class Json2Schema {
  // Parser to parse json if provided as string.
  private final JsonParser parser;

  public Json2Schema() {
    this.parser = new JsonParser();
  }

  /**
   * Generates a {@link Schema} given a row.
   *
   * @param id Schema id
   * @return returns {@link Schema}
   * @throws UnsupportedTypeException
   * @throws JSONException
   */
  public Schema toSchema(String id, Row row) throws RecordConvertorException {
    List<Schema.Field> fields = new ArrayList<>();

    // Iterate through each field in the row.
    for (Pair<String, Object> column : row.getFields()) {
      String name = column.getFirst();
      Object value = column.getSecond();

      // First, we check if object is of simple type.
      if (value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Short ||
          value instanceof Double || value instanceof Float || value instanceof Boolean || value instanceof byte[]) {
        try {
          Schema schema = Schema.nullableOf(new SimpleSchemaGenerator().generate(value.getClass()));
          fields.add(Schema.Field.of(name, schema));
        } catch (UnsupportedTypeException e) {
          throw new RecordConvertorException(
            String.format("Unable to convert field '%s' to basic type.", name)
          );
        }
      }

      if (value instanceof BigDecimal) {
        Schema schema = Schema.nullableOf(Schema.of(Schema.Type.DOUBLE));
        fields.add(Schema.Field.of(name, schema));
      }

      if (value instanceof LocalDate) {
        Schema schema = Schema.nullableOf(Schema.of(Schema.LogicalType.DATE));
        fields.add(Schema.Field.of(name, schema));
      }

      if (value instanceof LocalTime) {
        Schema schema = Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS));
        fields.add(Schema.Field.of(name, schema));
      }

      if (value instanceof ZonedDateTime) {
        Schema schema = Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        fields.add(Schema.Field.of(name, schema));
      }

      // TODO - remove all the instaces of java.util.Date once all the directives support LogicalType.
      if (value instanceof Date || value instanceof java.sql.Date || value instanceof Time
        || value instanceof Timestamp) {
        Schema schema = Schema.nullableOf(Schema.of(Schema.Type.LONG));
        fields.add(Schema.Field.of(name, schema));
      }

      if (value instanceof Map) {
        Schema schema = Schema.nullableOf (Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)));
        fields.add(Schema.Field.of(name, schema));
      }

      if (value instanceof JsonElement) {
        Schema schema = toSchema(name, (JsonElement) value);
        fields.add(Schema.Field.of(name, schema));
      }

      if (value instanceof Schema) {
        fields.addAll(((Schema) value).getFields());
      }
    }
    return Schema.recordOf(id, fields);
  }

  /**
   * Converts a json string to a {@link Schema}.
   *
   * @param id of the {@link Schema}
   * @param json represents the JSON as {@link String}
   * @return instance of {@link Schema} representing the JSON.
   * @throws RecordConvertorException throw if there are any issues parsing to {@link Schema}
   * @see {@link #toSchema(String, Row)}
   */
  public Schema toSchema(String id, String json) throws RecordConvertorException {
    JsonElement element = parser.parse(json);
    return toSchema(id, element);
  }

  @Nullable
  private Schema toSchema(String name, JsonElement element) throws RecordConvertorException {
    Schema schema = null;
    if (element == null) {
      return null;
    }
    if (element.isJsonObject() || element.isJsonArray()) {
      schema = toComplexSchema(name, element);
    } else if (element.isJsonPrimitive()) {
      schema = toSchema(name, element.getAsJsonPrimitive());
    } else if (element.isJsonNull()) {
      schema = Schema.of(Schema.Type.NULL);
    }
    return schema;
  }

  private Schema toSchema(String name, JsonArray array) throws RecordConvertorException {
    int[] types = new int[3];
    types[0] = types[1] = types[2] = 0;
    for(int i = 0; i < array.size(); ++i) {
      JsonElement item = array.get(i);
      if (item.isJsonArray()) {
        types[1]++;
      } else if (item.isJsonPrimitive()) {
        types[0]++;
      } else if (item.isJsonObject()) {
        types[2]++;
      }
    }

    int sum = types[0] + types[1] + types[2];
    if (sum > 0) {
      JsonElement child = array.get(0);
      if (types[2] > 0 || types[1] > 0) {
        return Schema.nullableOf(Schema.arrayOf(toComplexSchema(name, child)));
      } else if (types[0] > 0) {
        return Schema.nullableOf(Schema.arrayOf(toSchema(name, child.getAsJsonPrimitive())));
      }
    }
    return Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL)));
  }

  private Schema toSchema(String name, JsonPrimitive primitive) throws RecordConvertorException {
    Object value = JsParser.getValue(primitive);
    try {
      return Schema.nullableOf(new SimpleSchemaGenerator().generate(value.getClass()));
    } catch (UnsupportedTypeException e) {
      throw new RecordConvertorException(
        String.format("Unable to convert field '%s' to basic type.", name)
      );
    }
  }

  private Schema toSchema(String name, JsonObject object) throws RecordConvertorException {
    List<Schema.Field> fields = new ArrayList<>();
    Iterator<Map.Entry<String, JsonElement>> iterator = object.entrySet().iterator();
    while(iterator.hasNext()) {
      Map.Entry<String, JsonElement> next = iterator.next();
      String key = next.getKey();
      JsonElement child = next.getValue();
      Schema schema = null;
      if (child.isJsonObject() || child.isJsonArray()) {
        schema = toComplexSchema(key, child);
      } else if (child.isJsonPrimitive()) {
        schema = toSchema(key, child.getAsJsonPrimitive());
      }
      if (schema != null) {
        fields.add(Schema.Field.of(key, schema));
      }
    }
    if (fields.size() == 0) {
      return Schema.nullableOf(Schema.arrayOf(Schema.of(Schema.Type.NULL)));
    }
    return Schema.recordOf(name, fields);
  }

  private Schema toComplexSchema(String name, JsonElement element) throws RecordConvertorException {
    if (element.isJsonObject()) {
      return toSchema(name, element.getAsJsonObject());
    } else if (element.isJsonArray()){
      return toSchema(name, element.getAsJsonArray());
    } else if (element.isJsonPrimitive()) {
      return toSchema(name, element.getAsJsonPrimitive());
    }
    return null;
  }


  private static final class SimpleSchemaGenerator extends AbstractSchemaGenerator {
    @Override
    protected Schema generateRecord(TypeToken<?> typeToken, Set<String> set,
                                    boolean b) throws UnsupportedTypeException {
      // we don't actually leverage this method for types we support, so no need to implement it
      throw new UnsupportedTypeException(String.format("Generating record of type %s is not supported.",
                                                       typeToken));
    }
  }
}
