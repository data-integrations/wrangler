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

package co.cask.wrangler.internal;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.AbstractSchemaGenerator;
import co.cask.wrangler.api.Record;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Converts {@link Record} to {@link StructuredRecord}.
 */
public final class RecordConvertor implements Serializable {
  /**
   * Converts a Wrangler {@link Record} into a {@link StructuredRecord}.
   *
   * @param record defines a single {@link Record}
   * @param schema Schema associated with {@link StructuredRecord}
   * @return Populated {@link StructuredRecord}
   */
  public StructuredRecord toStructureRecord(Record record, Schema schema) throws RecordConvertorException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = schema.getFields();
    for (Schema.Field field : fields) {
      String name = field.getName();
      Object value = record.getValue(name);
      builder.set(name, decode(name, value, field.getSchema()));
    }
    return builder.build();
  }

  /**
   * Converts a list of {@link Record} into populated list of {@link StructuredRecord}
   *
   * @param records Collection of records.
   * @param schema Schema associated with {@link StructuredRecord}
   * @return Populated list of {@link StructuredRecord}
   */
  public List<StructuredRecord> toStructureRecord(List<Record> records, Schema schema) throws RecordConvertorException {
    List<StructuredRecord> results = new ArrayList<>();
    for (Record record : records) {
      StructuredRecord r = toStructureRecord(record, schema);
      results.add(r);
    }
    return results;
  }

  /**
   * Generates a schema given a list of records.
   *
   * @param id Schema id
   * @param records List of records.
   * @return returns {@link Schema}
   * @throws UnsupportedTypeException
   * @throws JSONException
   */
  public Schema toSchema(String id, List<Record> records) throws RecordConvertorException {
    List<Schema.Field> fields = new ArrayList<>();
    Record record = createUberRecord(records);

    // Iterate through each field in the record.
    for (KeyValue<String, Object> column : record.getFields()) {
      String name = column.getKey();
      Object value = column.getValue();

      // First, we check if object is of simple type.
      if (value instanceof String || value instanceof Integer ||
        value instanceof Long || value instanceof Short ||
        value instanceof Double || value instanceof Float ||
        value instanceof Boolean || value instanceof byte[]) {
        try {
          fields.add(
            Schema.Field.of(
              name,
              Schema.nullableOf(new SimpleSchemaGenerator().generate(value.getClass()))
            )
          );
        } catch (UnsupportedTypeException e) {
          throw new RecordConvertorException(
            String.format("Unable to convert field '%s' to basic type.", name)
          );
        }
      }

      // Check if the object is of map type, if it's of map type, then generate a schema for
      // map.
      if (value instanceof Map) {
        fields.add(
          Schema.Field.of(
            name,
            Schema.nullableOf (
              Schema.mapOf(
                Schema.of(Schema.Type.STRING),
                Schema.of(Schema.Type.STRING)
              )
            )
          )
        );
      }

      // We now check if it's of JSONArray Type.
      if (value instanceof JSONArray) {
        // If it's on array type, then we need to see the type of object the array has.
        JSONArray array = (JSONArray) value;
        if (array.length() > 0) {
          Schema arraySchema = null;
          try {
            arraySchema = generateJSONArraySchema(array);
          } catch (UnsupportedOperationException | UnsupportedTypeException e) {
            throw new RecordConvertorException(
              String.format("Unable to generate schema for field '%s'. Complex JSON objects not supported yet", name)
            );
          }
          fields.add(
            Schema.Field.of(
              name,
              Schema.nullableOf(
                arraySchema
              )
            )
          );
        }
      }
    }
    // Construct the final Schema adding all the fields.
    return Schema.recordOf(id, fields);
  }

  private Object decode(String name, Object object, Schema schema) throws RecordConvertorException {
    // Extract the type of the field.
    Schema.Type type = schema.getType();

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
        if (object instanceof JSONArray) {
          return decodeJSONArray(name, (JSONArray) object, schema.getComponentSchema());
        } else {
          return decodeArray(name, (List) object, schema.getComponentSchema());
        }
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

  private Object decodeJSONArray(String name, JSONArray object, Schema schema) throws RecordConvertorException {
    Schema.Type type = schema.getType();
    if (type == Schema.Type.RECORD) {
      List<Object> records = Lists.newArrayListWithCapacity(object.length());
      for (int i = 0; i < object.length(); ++i) {
        records.add(decodeRecord(name, object.getJSONObject(i), schema));
      }
      return records;
    } else {
      List<Object> records = Lists.newArrayListWithCapacity(object.length());
      for (int i = 0; i < object.length(); ++i) {
        records.add(decode(name, object.get(i), schema));
      }
      return records;
    }
  }

  @SuppressWarnings("RedundantCast")
  private Object decodeSimpleTypes(String name, Object object, Schema schema) throws RecordConvertorException {
    Schema.Type type = schema.getType();
    if (object == null || JSONObject.NULL.equals(object)) {
      return null;
    }
    switch (type) {
      case NULL:
        return null; // nothing much to do here.
      case INT:
        if (object instanceof Integer) {
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
            String.format("Schema specifies field '%s' is integer, but the value is nor a integer or string. " +
                            "It is of type '%s'", name, object.getClass().getName())
          );
        }
      case LONG:
        if (object instanceof Long) {
          return (Long) object;
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

  private List<Object> decodeArray(String name, List nativeArray, Schema schema) throws RecordConvertorException {
    List<Object> array = Lists.newArrayListWithCapacity(nativeArray.size());
    for (Object object : nativeArray) {
      array.add(decode(name, object, schema));
    }
    return array;
  }

  private StructuredRecord decodeRecord(String name, JSONObject object, Schema schema)
    throws RecordConvertorException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      try {
        Object fieldVal = object.get(fieldName);
        builder.set(fieldName, decode(name, fieldVal, field.getSchema()));
      } catch (JSONException e) {
        // High chance that field does not exist.
      }
    }
    return builder.build();
  }

  private static Record createUberRecord(List<Record> records) {
    Record uber = new Record();
    for (Record record : records) {
      for (int i = 0; i < record.length(); ++i) {
        Object o = record.getValue(i);
        uber.addOrSet(record.getColumn(i), null);
        if (o != null) {
          uber.addOrSet(record.getColumn(i), o);
        }
      }
    }
    return uber;
  }

  private static Schema generateJSONArraySchema(JSONArray array)
    throws JSONException, UnsupportedTypeException, UnsupportedOperationException, RecordConvertorException {
    Object object = array.get(0);

    // If it's not JSONObject, it's simple type.
    if (!(object instanceof JSONObject)) {
      return Schema.arrayOf(
        new SimpleSchemaGenerator().generate(object.getClass())
      );
    } else {
      // It's JSONObject, now we need to determine types of it.
      Map<String, Object> types = getJSONArraySchema(array);
      List<Schema.Field> fields = new ArrayList<>();
      for (Map.Entry<String, Object> entry : types.entrySet()) {
        String nm = entry.getKey();
        Object obj = entry.getValue();
        fields.add(
          Schema.Field.of(
            nm,
            Schema.nullableOf(
              new SimpleSchemaGenerator().generate(obj.getClass())
            )
          )
        );
      }
      // We have collected all the fields from the object. Now,
      // we add it as array of these objects.
      return Schema.arrayOf(Schema.recordOf("subrecord", fields));
    }
  }

  private static Map<String, Object> getJSONArraySchema(JSONArray array)
    throws JSONException, RecordConvertorException {
    Map<String, Object> types = new HashMap<>();
    for (int i = 0; i < array.length(); ++i) {
      JSONObject object = (JSONObject) array.get(i);
      getJSONObjectSchema(object, types);
    }
    return types;
  }

  private static void getJSONObjectSchema(JSONObject object, Map<String, Object> types)
    throws JSONException, RecordConvertorException {
    Iterator<String> it = object.keys();
    while(it.hasNext()) {
      String name = it.next();
      Object value = object.get(name);
      if (value instanceof JSONArray || value instanceof JSONObject) {
        throw new RecordConvertorException(
          String.format("Current version does not support complex nested types of JSON. Please flatten the JSON " +
                          "using PARSE-AS-JSON directive.")
        );
      }
      if (!JSONObject.NULL.equals(value)) {
        types.put(name, value);
      }
    }
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
