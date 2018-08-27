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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.directives.parser.JsParser;
import co.cask.wrangler.api.Row;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Converts {@link Row} to {@link StructuredRecord}.
 */
public final class RecordConvertor implements Serializable {

  /**
   * Converts a list of {@link Row} into populated list of {@link StructuredRecord}
   *
   * @param rows Collection of rows.
   * @param schema Schema associated with {@link StructuredRecord}
   * @return Populated list of {@link StructuredRecord}
   */
  public List<StructuredRecord> toStructureRecord(List<Row> rows, Schema schema) throws RecordConvertorException {
    List<StructuredRecord> results = new ArrayList<>();
    for (Row row : rows) {
      StructuredRecord r = decodeRecord(row, schema);
      results.add(r);
    }
    return results;
  }

  /**
   * Converts a Wrangler {@link Row} into a {@link StructuredRecord}.
   *
   * @param row defines a single {@link Row}
   * @param schema Schema associated with {@link StructuredRecord}
   * @return Populated {@link StructuredRecord}
   */
  public StructuredRecord decodeRecord(Row row, Schema schema) throws RecordConvertorException {
    // TODO: This is a hack to workaround StructuredRecord processing. NEED TO RETHINK.
    if (row.getFields().size() == 1) {
      Object cell = row.getValue(0);
      if (cell instanceof StructuredRecord) {
        return (StructuredRecord) cell;
      }
    }
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      Schema fSchema = field.getSchema();
      boolean isNullable = fSchema.isNullable();
      String name = field.getName();
      Object value = row.getValue(name);
      try {
        Object decodedObj = decode(name, value, field.getSchema());
        if (decodedObj instanceof LocalDate) {
          builder.setDate(name, (LocalDate) decodedObj);
        } else if (decodedObj instanceof LocalTime) {
          builder.setTime(name, (LocalTime) decodedObj);
        } else if (decodedObj instanceof ZonedDateTime) {
          builder.setTimestamp(name, (ZonedDateTime) decodedObj);
        } else {
          builder.set(name, decodedObj);
        }
      } catch (UnexpectedFormatException e) {
        throw new RecordConvertorException(
          String.format("Field '%s' of type '%s' (Nullable : '%s') cannot be set to '%s'. Possibly the value being " +
                          "set is not in inline with the schema specified. Check schema for field '%s'.",
                        name,
                        isNullable ? fSchema.getNonNullable().getType().name() : fSchema.getType().name(),
                        isNullable ? "YES" : "NO",
                        value == null ? "NULL" : value,
                        name)
        );
      }
    }
    return builder.build();
  }

  private Object decode(String name, Object object, Schema schema) throws RecordConvertorException {
    // Extract the type of the field.
    Schema.Type type = schema.getType();
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
        case TIME_MILLIS:
        case TIME_MICROS:
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return object;
        default:
          throw new UnexpectedFormatException("field type " + logicalType + " is not supported.");
      }
    }

    // Now based on the type, do the necessary decoding.
    switch (type) {
      case NULL:
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        return decodeSimpleTypes(name, object, schema);
      case ENUM:
        break;
      case ARRAY:
        return decodeArray(name, object, schema.getComponentSchema());
      case RECORD:
        return decodeRecord(name, object, schema);
      case MAP:
        Schema key = schema.getMapSchema().getKey();
        Schema value = schema.getMapSchema().getValue();
        // Should be fine to cast since schema tells us what it is.
        // noinspection unchecked
        return decodeMap(name, (Map<Object, Object>) object, key, value);
      case UNION:
        return decodeUnion(name, object, schema.getUnionSchemas());
    }

    throw new RecordConvertorException(
      String.format("Unable decode object '%s' with schema type '%s'.", name, type.toString())
    );
  }

  private StructuredRecord decodeRecord(String name, Object object, Schema schema) throws RecordConvertorException {
    if (object instanceof Map) {
      return decodeRecord(name, (Map) object, schema);
    } else if (object instanceof JsonObject) {
      return decodeRecord(name, (JsonObject) object, schema);
    } else if (object instanceof JsonArray) {
      List<Object> values = decodeArray(name, object, schema.getComponentSchema());
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      builder.set(name, values);
      return builder.build();
    }
    throw new RecordConvertorException(
      String.format("Unable decode object '%s' with schema type '%s'.", name, schema.getType().toString())
    );
  }

  private StructuredRecord decodeRecord(String name,
                                        JsonObject nativeObject, Schema schema) throws RecordConvertorException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object fieldVal = nativeObject.get(fieldName);
      builder.set(fieldName, decode(name, fieldVal, field.getSchema()));
    }
    return builder.build();
  }

  private StructuredRecord decodeRecord(String name, Map nativeObject, Schema schema) throws RecordConvertorException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object fieldVal = nativeObject.get(fieldName);
      builder.set(fieldName, decode(name, fieldVal, field.getSchema()));
    }
    return builder.build();
  }

  @SuppressWarnings("RedundantCast")
  private Object decodeSimpleTypes(String name, Object object, Schema schema) throws RecordConvertorException {
    Schema.Type type = schema.getType();

    if (object == null || JsonNull.INSTANCE.equals(object)) {
      return null;
    } else if (object instanceof JsonPrimitive) {
      return JsParser.getValue((JsonPrimitive) object);
    } else if (type != Schema.Type.STRING && object instanceof String) {
      // Data prep can convert string to other primitive types. if the value is empty for non-string primitive return null
      String val = (String) object;
      if (val.trim().isEmpty()) {
        return null;
      }
    }

    switch (type) {
      case NULL:
        return null; // nothing much to do here.
      case INT:
        if (object instanceof Integer || object instanceof Short) {
          return (Integer) object;
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            return Integer.parseInt(value);
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to integer for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is integer, but the value is not a integer or string. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
      case LONG:
        if (object instanceof Long) {
          return (Long) object;
        } else if (object instanceof Integer) {
          return ((Integer) object).longValue();
        } else if (object instanceof Short) {
          return ((Short) object).longValue();
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            return Long.parseLong(value);
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to long for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is long, but the value is nor a string or long. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
      case FLOAT:
        if (object instanceof Float) {
          return (Float) object;
        } else if (object instanceof Long) {
          return ((Long) object).floatValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).floatValue();
        } else if (object instanceof Short) {
          return ((Short) object).floatValue();
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            return Float.parseFloat(value);
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to float for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is float, but the value is nor a string or float. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
      case DOUBLE:
        if (object instanceof Double) {
          return (Double) object;
        } else if (object instanceof BigDecimal) {
          return ((BigDecimal) object).doubleValue();
        } else if (object instanceof Float) {
          return ((Float) object).doubleValue();
        } else if (object instanceof Long) {
          return ((Long) object).doubleValue();
        } else if (object instanceof Integer) {
          return ((Integer) object).doubleValue();
        } else if (object instanceof Short) {
          return ((Short) object).doubleValue();
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            return Double.parseDouble(value);
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to double for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is double, but the value is nor a string or double. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
      case BOOLEAN:
        if (object instanceof Boolean) {
          return (Boolean) object;
        } else if (object instanceof String) {
          String value = (String) object;
          try {
            return Boolean.parseBoolean(value);
          } catch (NumberFormatException e) {
            throw new RecordConvertorException(
              String.format("Unable to convert '%s' to boolean for field name '%s'", value, name)
            );
          }
        } else {
          throw new RecordConvertorException(
            String.format("Schema specifies field '%s' is double, but the value is nor a string or boolean. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }

      case STRING:
        return object.toString();

      case BYTES:
        if (object instanceof byte[]) {
          return (byte[]) object;
        } else if (object instanceof Boolean) {
          return Bytes.toBytes((Boolean) object);
        } else if (object instanceof Double) {
          return Bytes.toBytes((Double) object);
        } else if (object instanceof Float) {
          return Bytes.toBytes((Float) object);
        } else if (object instanceof Long) {
          return Bytes.toBytes((Long) object);
        } else if (object instanceof Integer) {
          return Bytes.toBytes((Integer) object);
        } else if (object instanceof Short) {
          return Bytes.toBytes((Short) object);
        } else if (object instanceof String) {
          return Bytes.toBytes((String) object);
        } else if (object instanceof BigDecimal) {
          return Bytes.toBytes((BigDecimal) object);
        } else {
          throw new RecordConvertorException(
            String.format("Unable to convert '%s' to bytes for field name '%s'", object.toString(), name)
          );
        }
    }
    throw new RecordConvertorException(
      String.format("Unable decode object '%s' with schema type '%s'.", name, type.toString())
    );
  }

  private Map<Object, Object> decodeMap(String name,
                                        Map<Object, Object> object, Schema key, Schema value)
    throws RecordConvertorException {
    Map<Object, Object> output = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : object.entrySet()) {
      output.put(decode(name, entry.getKey(), key), decode(name, entry.getValue(), value));
    }
    return output;
  }

  private Object decodeUnion(String name, Object object, List<Schema> schemas) throws RecordConvertorException {
    for (Schema schema : schemas) {
      return decode(name, object, schema);
    }
    throw new RecordConvertorException(
      String.format("Unable decode object '%s'.", name)
    );
  }

  private List<Object> decodeArray(String name, Object object, Schema schema) throws RecordConvertorException {
    if (object instanceof List) {
      return decodeArray(name, (List) object, schema);
    } else if (object instanceof JsonArray) {
      return decodeArray(name, (JsonArray) object, schema);
    }
    throw new RecordConvertorException(
      String.format("Unable to decode array '%s'", name)
    );
  }

  private List<Object> decodeArray(String name, JsonArray list, Schema schema) throws RecordConvertorException {
    List<Object> array = Lists.newArrayListWithCapacity(list.size());
    for (int i = 0; i < list.size(); ++i) {
      array.add(decode(name, list.get(i), schema));
    }
    return array;
  }

  private List<Object> decodeArray(String name, List list, Schema schema) throws RecordConvertorException {
    List<Object> array = Lists.newArrayListWithCapacity(list.size());
    for (Object object : array) {
      array.add(decode(name, object, schema));
    }
    return array;
  }
}
