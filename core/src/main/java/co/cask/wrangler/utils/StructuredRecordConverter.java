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

package co.cask.wrangler.utils;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

/**
 * Converts an {@link StructuredRecord} to subset of the input {@link StructuredRecord} based
 * on the {@link Schema} specified.
 */
public final class StructuredRecordConverter {

  public static StructuredRecord transform(StructuredRecord record, Schema schema) throws IOException {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    for (Schema.Field field : record.getSchema().getFields()) {
      String name = field.getName();
      // If the field name is not in the output, then skip it and if it's not nullable, then
      // it would be error out -- in which case, the user has to fix the schema to proceed.
      if (schema.getField(name) != null) {
        builder.set(name, convertField(record.get(name), field.getSchema()));
      }
    }
    return builder.build();
  }

  private static Object convertUnion(Object value, List<Schema> schemas) {
    boolean isNullable = false;
    for (Schema possibleSchema : schemas) {
      if (possibleSchema.getType() == Schema.Type.NULL) {
        isNullable = true;
        if (value == null) {
          return value;
        }
      } else {
        try {
          return convertField(value, possibleSchema);
        } catch (Exception e) {
          // if we couldn't convert, move to the next possibility
        }
      }
    }
    if (isNullable) {
      return null;
    }
    throw new UnexpectedFormatException("Unable to determine the union type.");
  }

  private static List<Object> convertArray(Object values, Schema elementSchema) throws IOException {
    List<Object> output;
    if (values instanceof List) {
      List<Object> valuesList = (List<Object>) values;
      output = Lists.newArrayListWithCapacity(valuesList.size());
      for (Object value : valuesList) {
        output.add(convertField(value, elementSchema));
      }
    } else {
      int length = Array.getLength(values);
      output = Lists.newArrayListWithCapacity(length);
      for (int i = 0; i < length; i++) {
        output.add(convertField(Array.get(values, i), elementSchema));
      }
    }
    return output;
  }

  private static Map<Object, Object> convertMap(Map<Object, Object> map,
                                         Schema keySchema, Schema valueSchema) throws IOException {
    Map<Object, Object> converted = Maps.newHashMap();
    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      converted.put(convertField(entry.getKey(), keySchema), convertField(entry.getValue(), valueSchema));
    }
    return converted;
  }

  protected static Object convertField(Object field, Schema fieldSchema) throws IOException {
    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case RECORD:
        return transform((StructuredRecord) field, fieldSchema);
      case ARRAY:
        return convertArray(field, fieldSchema.getComponentSchema());
      case MAP:
        Map.Entry<Schema, Schema> mapSchema = fieldSchema.getMapSchema();
        return convertMap((Map<Object, Object>) field, mapSchema.getKey(), mapSchema.getValue());
      case UNION:
        return convertUnion(field, fieldSchema.getUnionSchemas());
      case NULL:
        return null;
      case STRING:
        return field.toString();
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return field;
      default:
        throw new UnexpectedFormatException("Unsupported field type " + fieldType);
    }
  }
}