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
import com.google.gson.JsonPrimitive;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.Field;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.internal.guava.reflect.TypeToken;
import io.cdap.cdap.internal.io.AbstractSchemaGenerator;
import io.cdap.directives.parser.JsParser;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import org.json.JSONException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * This class provides functionality to retrieve  {@link Schema} from a row or an object.
 */
public final class SchemaConverter {

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

      if (value instanceof Schema) {
        fields.addAll(((Schema) value).getFields());
      } else {
        Schema schema = getSchema(value, name);
        if (schema != null) {
          fields.add(Schema.Field.of(name, schema));
        }
      }
    }
    return Schema.recordOf(id, fields);
  }

  /**
   * Generates the CDAP Schema for the given object
   *
   * @param value the value to retrieve schema
   * @param name name of the field
   * @return the schema of this object
   */
  @Nullable
  public Schema getSchema(Object value, String name) throws RecordConvertorException {
    return getSchema(value, name, null);
  }

  /**
   * Generates the CDAP Schema for the given object
   *
   * @param value the value to retrieve schema
   * @param name name of the field
   * @param recordPrefix prefix to append at the beginning of a custom record
   * @return the schema of this object
   * NOTE: ANY NEWLY SUPPORTED DATATYPE SHOULD ALSO BE REGISTERED IN {@link KryoSerializer}
   */
  @Nullable
  public Schema getSchema(Object value, String name, @Nullable String recordPrefix) throws RecordConvertorException {
    // First, we check if object is of simple type.
    if (value instanceof String || value instanceof Integer || value instanceof Long || value instanceof Short ||
      value instanceof Double || value instanceof Float || value instanceof Boolean || value instanceof byte[] ||
      value instanceof ByteBuffer) {
      try {
        return Schema.nullableOf(new SimpleSchemaGenerator().generate(value.getClass()));
      } catch (UnsupportedTypeException e) {
        throw new RecordConvertorException(
          String.format("Unable to convert field '%s' to basic type.", name), e);
      }
    }

    if (value instanceof BigDecimal) {
      // TODO CDAP-15361 precision should be derived from the schema of the row.
      return Schema.nullableOf(Schema.decimalOf(38, ((BigDecimal) value).scale()));
    }

    if (value instanceof LocalDate) {
      return Schema.nullableOf(Schema.of(Schema.LogicalType.DATE));
    }

    if (value instanceof LocalTime) {
      return Schema.nullableOf(Schema.of(Schema.LogicalType.TIME_MICROS));
    }

    if (value instanceof ZonedDateTime) {
      return Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
    }

    if (value instanceof LocalDateTime) {
      return Schema.nullableOf(Schema.of(LogicalType.DATETIME));
    }

    // TODO - remove all the instaces of java.util.Date once all the directives support LogicalType.
    if (value instanceof Date || value instanceof java.sql.Date || value instanceof Time
      || value instanceof Timestamp) {
      return Schema.nullableOf(Schema.of(Schema.Type.LONG));
    }

    if (value instanceof Map) {
      return Schema.nullableOf(Schema.mapOf(Schema.of(Schema.Type.STRING), Schema.of(Schema.Type.STRING)));
    }

    if (value instanceof JsonElement) {
      return toSchema(name, (JsonElement) value);
    }

    if (value instanceof Collection) {
      Collection<?> collection = (Collection) value;
      for (Object listObject : collection) {
        if (listObject == null) {
          continue;
        }

        Schema schema = getSchema(listObject, name, recordPrefix);
        // this means schema is unknown and is not supported.
        if (schema == null) {
          return null;
        }
        return Schema.nullableOf(Schema.arrayOf(schema));
      }
    }

    if (value instanceof Row) {
      List<Field> fields = new ArrayList<>();
      String recordTypeName = "";
      if (recordPrefix != null) {
        recordTypeName = recordPrefix + ".";
      }
      recordTypeName = recordTypeName + name;
      for (Pair<String, Object> field: ((Row) value).getFields()) {
        if (field.getSecond() != null) {
          fields.add(Field.of(field.getFirst(), getSchema(field.getSecond(), field.getFirst(), recordTypeName)));
        }
      }
      Schema namingSchema = Schema.recordOf(fields);
      recordTypeName = recordTypeName + namingSchema.getRecordName();
      return Schema.nullableOf(Schema.recordOf(recordTypeName, fields));
    }

    return null;
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
    return toSchema(name, array, null);
  }

  private Schema toSchema(String name, JsonArray array, String recordPrefix) throws RecordConvertorException {
    int[] types = new int[3];
    types[0] = types[1] = types[2] = 0;
    for (int i = 0; i < array.size(); ++i) {
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
        return Schema.nullableOf(Schema.arrayOf(toComplexSchema(name, child, recordPrefix)));
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
        String.format("Unable to convert field '%s' to basic type.", name), e);
    }
  }

  private Schema toSchema(String name, JsonObject object) throws RecordConvertorException {
    return toSchema(name, object, null);
  }

  private Schema toSchema(String name, JsonObject object, @Nullable String recordPrefix)
    throws RecordConvertorException {
    List<Schema.Field> fields = new ArrayList<>();

    String recordTypeName = "";
    if (recordPrefix != null) {
      recordTypeName = recordPrefix + ".";
    }
    recordTypeName = recordTypeName + name;

    for (Map.Entry<String, JsonElement> next : object.entrySet()) {
      String key = next.getKey();
      JsonElement child = next.getValue();
      Schema schema = null;
      if (child.isJsonObject() || child.isJsonArray()) {
        schema = toComplexSchema(key, child, recordTypeName);
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
    Schema namingSchema = Schema.recordOf(fields);
    recordTypeName = recordTypeName + namingSchema.getRecordName();
    return Schema.recordOf(recordTypeName, fields);
  }

  private Schema toComplexSchema(String name, JsonElement element) throws RecordConvertorException {
    return toComplexSchema(name, element, null);
  }

  private Schema toComplexSchema(String name,
                                 JsonElement element, @Nullable String recordPrefix) throws RecordConvertorException {
    if (element.isJsonObject()) {
      return toSchema(name, element.getAsJsonObject(), recordPrefix);
    } else if (element.isJsonArray()) {
      return toSchema(name, element.getAsJsonArray(), recordPrefix);
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
