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

package co.cask.wrangler.service;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.internal.io.AbstractSchemaGenerator;
import co.cask.wrangler.api.Record;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema useful utilities.
 *
 * This class is package private and cannot be used outside this package.
 */
class SchemaUtilities {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaUtilities.class);

  public static Schema recordToSchema(String id, List<Record> records) throws UnsupportedTypeException, JSONException {
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
        fields.add(
          Schema.Field.of(
            name,
            Schema.nullableOf(new SimpleSchemaGenerator().generate(value.getClass()))
          )
        );
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
          Schema arraySchema = generateJSONArraySchema(array);
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
    throws JSONException, UnsupportedTypeException {
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

  private static Map<String, Object> getJSONArraySchema(JSONArray array) throws JSONException {
    Map<String, Object> types = new HashMap<>();
    for (int i = 0; i < array.length(); ++i) {
      JSONObject object = (JSONObject) array.get(i);
      getJSONObjectSchema(object, types);
    }
    return types;
  }

  private static void getJSONObjectSchema(JSONObject object, Map<String, Object> types) throws JSONException {
    Iterator<String> it = object.keys();
    while(it.hasNext()) {
      String name = it.next();
      Object value = object.get(name);
      if (value != JSONObject.NULL) {
        types.put(name, value);
      }
    }
  }

  private static final class SimpleSchemaGenerator extends AbstractSchemaGenerator {
    @Override
    protected Schema generateRecord(TypeToken<?> typeToken, Set<String> set,
                                    boolean b) throws UnsupportedTypeException {
      if (typeToken.getType() instanceof JSONObject) {

      }
      // we don't actually leverage this method, so no need to implement it
      throw new UnsupportedOperationException();
    }
  }
}
