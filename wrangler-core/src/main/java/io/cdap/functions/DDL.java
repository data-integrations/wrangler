/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.functions;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.gson.JsonElement;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.utils.StructuredRecordJsonConverter;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Structured Row and Schema DDL.
 */
public final class DDL {
  private DDL() {
  }

  /**
   * Given a JSON representation of schema, returns a {@link Schema} object.
   *
   * @param json string representation of schema.
   * @return instance of {@link Schema} representing JSON string.
   * @throws IOException thrown when there is issue parsing the data.
   */
  public static Schema parse(String json) throws IOException {
    return Schema.parseJson(json);
  }

  /**
   * Given a SQL DDL, returns the {@link Schema} representation of the SQL.
   *
   * @param sql to be converted to {@link Schema}.
   * @return instance of {@link Schema}.
   * @throws IOException throw when there is issue parsing the sql.
   */
  public static Schema parsesql(String sql) throws IOException {
    return Schema.parseSQL(sql);
  }

  /**
   * Identity parse method to extract {@link Schema}.
   *
   * This method is added if a user makes a mistake of parsing the schema again.
   *
   * @param schema to be returned.
   * @return above schema object.
   */
  public static Schema parse(Schema schema) {
    return schema;
  }

  /**
   * Given a {@link StructuredRecord} returns the same {@link StructuredRecord}.
   */
  public static StructuredRecord parse(StructuredRecord record) {
    return record;
  }

  /**
   * Converts a {@link StructuredRecord} to JSON string.
   *
   * @param record to be converted to JSON
   * @return JSON string representation of structured record.
   * @throws IOException thrown if there are any issues with parsing.
   */
  public static JsonElement toJson(StructuredRecord record) throws IOException {
    return JsonFunctions.Parse(StructuredRecordJsonConverter.toJsonString(record));
  }

  /**
   * Given a {@link StructuredRecord} and the name of the field, it checks, if the
   * field exists in the {@link StructuredRecord}.
   *
   * @param record in which the field needs to be checked.
   * @param name of the field to be checked.
   * @return true if present and it's a record, else false.
   */
  public static boolean hasField(StructuredRecord record, String name) {
    Schema.Field field = record.getSchema().getField(name);
    if (field == null) {
      return false;
    }
    return true;
  }

  /**
   * Drops a path from the schema to generate a new {@link Schema}.
   *
   * @param schema to be modified.
   * @param path to be removed from {@link Schema}.
   * @return modified {@link Schema}
   */
  public static Schema drop(Schema schema, String path) {
    return drop(schema, path, path);
  }

  public static Schema drop(Schema schema, String path, String ... paths) {
    Schema mutatedSchema = drop(schema, path, path);
    for (String otherPath : paths) {
      mutatedSchema = drop(mutatedSchema, otherPath, otherPath);
    }
    return mutatedSchema;
  }

  private static Schema drop(Schema schema, @Nullable String path, String fullPath) {
    if (path == null) {
      return schema;
    }

    // TODO: Need to handle union type
    if (schema.getType() != Schema.Type.RECORD) {
      return schema;
    }

    int dotIndex = path.indexOf('.');
    String recordField = dotIndex > 0 ? path.substring(0, dotIndex) : path;
    String nextPath = dotIndex > 0 ? path.substring(dotIndex + 1) : null;

    int arrayIndex = -1;
    int bracketIndex = recordField.indexOf('[');
    if (bracketIndex > 0) {
      if (!recordField.endsWith("]")) {
        throw new IllegalArgumentException(
          String.format("Invalid field '%s' in path '%s'. An array index start with '[' and must end with a ']'",
                        recordField, fullPath));
      }
      String indexStr = recordField.substring(bracketIndex + 1, recordField.length() - 1);
      try {
        arrayIndex = Integer.parseInt(indexStr);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
          String.format("Invalid array index '%s' for field '%s' in path '%s'. Must be a valid integer.",
                        indexStr, recordField, fullPath));
      }
      recordField = recordField.substring(0, bracketIndex);
    }

    List<Schema.Field> fields = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      if (field.getName().equals(recordField)) {
        if (nextPath == null) {
          continue;
        }
        Schema fieldSchema = field.getSchema();
        boolean isNullable = fieldSchema.isNullable();
        Schema.Type fieldtype = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (arrayIndex >= 0) {
          if (fieldtype != Schema.Type.ARRAY) {
            throw new IllegalArgumentException(
              String.format("Array syntax for nested field '%s' in path '%s' was specified, but '%s' is of type %s.",
                            recordField, fullPath, recordField, fieldtype));
          }
          Schema componentSchema =
            isNullable ? fieldSchema.getNonNullable().getComponentSchema() : fieldSchema.getComponentSchema();
          boolean componentNullable = componentSchema.isNullable();
          Schema.Type componentType = componentNullable ?
            componentSchema.getNonNullable().getType() : componentSchema.getType();
          if (nextPath != null && componentType != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
              String.format("Nested field '%s' in path '%s' must be of type '%s', but is '%s'.",
                            recordField, fullPath, Schema.Type.RECORD, componentType));
          }

          Schema componentSchemaWithout =
            drop(componentNullable ? componentSchema.getNonNullable() : fieldSchema, nextPath, fullPath);
          componentSchemaWithout =
            componentNullable ? Schema.nullableOf(componentSchemaWithout) : componentSchemaWithout;

          Schema arraySchema = Schema.arrayOf(componentSchemaWithout);
          arraySchema = isNullable ? Schema.nullableOf(arraySchema) : arraySchema;
          fields.add(Schema.Field.of(field.getName(), arraySchema));
          continue;
        } else if (nextPath != null && fieldtype != Schema.Type.RECORD) {
          throw new IllegalArgumentException(
            String.format("Nested field '%s' in path '%s' must be of type '%s', but is '%s'.",
                          recordField, fullPath, Schema.Type.RECORD, fieldtype));
        }
        fields.add(Schema.Field.of(field.getName(), drop(field.getSchema(), nextPath, fullPath)));
      } else {
        fields.add(field);
      }
    }
    return Schema.recordOf(schema.getRecordName(), fields);
  }

  /**
   * Selects the sub-record based on the path specified.
   *
   * @param schema to be used as source to apply the path to.
   * @param path to be applied on the {@link Schema} to select.
   * @return sub-selected {@link Schema}
   */
  public static Schema select(Schema schema, String path) {
    return select(schema, path, Splitter.on('.').split(path).iterator());
  }

  private static Schema select(Schema schema, String path, Iterator<String> fields) {
    String fieldName = fields.next();

    String key = null;
    int bracketIndex = fieldName.indexOf('[');
    if (bracketIndex > 0) {
      if (!fieldName.endsWith("]")) {
        throw new IllegalArgumentException(
          String.format("Invalid field '%s'. An index start '[' must end with a ']'", fieldName));
      }
      key = fieldName.substring(bracketIndex + 1, fieldName.length() - 1);
      fieldName = fieldName.substring(0, bracketIndex);
    }

    Schema.Field field = schema.getField(fieldName);

    if (field == null) {
      throw new IllegalArgumentException(
        String.format("Nested field '%s' in path '%s' does not exist", fieldName, path));
    }

    Schema fieldSchema = field.getSchema();
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    if (key != null) {
      if (fieldType == Schema.Type.ARRAY) {
        try {
          int arrayIndex = Integer.parseInt(key);
          if (arrayIndex < 0) {
            throw new IllegalArgumentException(
              String.format("Invalid array index '%d' for field '%s' in path '%s. Must be at least 0.",
                            arrayIndex, fieldName, path));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
            String.format("Invalid array index '%s' for field '%s' in path '%s'. Must be a valid int.",
                          key, fieldName, path));
        }

        fieldSchema = isNullable ? fieldSchema.getNonNullable().getComponentSchema() : fieldSchema.getComponentSchema();
        isNullable = fieldSchema.isNullable();
        fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      } else if (fieldType == Schema.Type.MAP) {

        Map.Entry<Schema, Schema> mapSchema = fieldSchema.getMapSchema();
        Schema keySchema = mapSchema.getKey();
        Schema valSchema = mapSchema.getValue();

        Schema.Type keyType = keySchema.isNullable() ? keySchema.getNonNullable().getType() : keySchema.getType();
        if (keyType != Schema.Type.STRING) {
          throw new IllegalArgumentException(
            String.format("Only map keys of type string are supported. Field '%s' in path '%s' has keys of type '%s'.",
                          fieldName, path, keyType));
        }

        fieldSchema = valSchema;
        isNullable = fieldSchema.isNullable();
        fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

      } else {
        throw new IllegalArgumentException(
          String.format("Array syntax for nested field '%s' in path '%s' was specified, but '%s' is of type %s.",
                        fieldName, path, fieldName, fieldType));
      }
    }

    if (fields.hasNext()) {
      if (fieldType != Schema.Type.RECORD) {
        throw new IllegalArgumentException(
          String.format("Nested field '%s' in path '%s' must be of type '%s', but is '%s'.",
                        fieldName, path, Schema.Type.RECORD, fieldType));
      }

      return select(isNullable ? fieldSchema.getNonNullable() : fieldSchema, path, fields);
    }
    return fieldSchema;
  }

  public static StructuredRecord drop(StructuredRecord record, String path) {
    return drop(record, path, path);
  }

  public static StructuredRecord drop(StructuredRecord record, String path, String ... paths) {
    // No-op. This is just to mirror the drop method for Schema
    return record;
  }

    /**
     * Select a sub-stucture of a {@link StructuredRecord}.
     *
     * @param record to be operated on
     * @param path to applied to the {@link StructuredRecord}
     * @param <T> type of object returned.
     * @return sub-structure of the {@link StructuredRecord}
     */
  public static <T> T select(StructuredRecord record, String path) {
    return recursiveGet(record, path, Splitter.on('.').split(path).iterator());
  }

  private static <T> T recursiveGet(StructuredRecord record, String path, Iterator<String> fields) {
    String fieldName = fields.next();

    int bracketIndex = fieldName.indexOf('[');
    String key = null;
    if (bracketIndex > 0) {
      if (!fieldName.endsWith("]")) {
        throw new IllegalArgumentException(
          String.format("Invalid field '%s'. An index start '[' must end with a ']'", fieldName));
      }
      key = fieldName.substring(bracketIndex + 1, fieldName.length() - 1);
      fieldName = fieldName.substring(0, bracketIndex);
    }

    Schema.Field field = record.getSchema().getField(fieldName);

    if (field == null) {
      throw new IllegalArgumentException(
        String.format("Nested field '%s' in path '%s' does not exist", fieldName, path));
    }

    Schema fieldSchema = field.getSchema();
    boolean isNullable = fieldSchema.isNullable();
    Schema.Type fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

    Object val = record.get(fieldName);

    if (key != null) {
      if (fieldType == Schema.Type.ARRAY) {
        try {
          int arrayIndex = Integer.parseInt(key);
          if (arrayIndex < 0) {
            throw new IllegalArgumentException(
              String.format("Invalid array index '%d' for field '%s' in path '%s. Must be at least 0.",
                            arrayIndex, fieldName, path));
          }

          fieldSchema = isNullable ?
            fieldSchema.getNonNullable().getComponentSchema() : fieldSchema.getComponentSchema();
          isNullable = fieldSchema.isNullable();
          fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

          try {
            if (val instanceof Collection) {
              val = Iterables.get((Collection) val, arrayIndex);
            } else {
              val = Array.get(val, arrayIndex);
            }
          } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
              String.format("Index '%d' for nested field '%s' in path '%s' is out of bounds.",
                            arrayIndex, fieldName, path));
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
            String.format("Invalid array index '%s' for field '%s' in path '%s'. Must be a valid int.",
                          key, fieldName, path));
        }
      } else if (fieldType == Schema.Type.MAP) {

        Map.Entry<Schema, Schema> mapSchema = fieldSchema.getMapSchema();
        Schema keySchema = mapSchema.getKey();
        Schema valSchema = mapSchema.getValue();

        Schema.Type keyType = keySchema.isNullable() ? keySchema.getNonNullable().getType() : keySchema.getType();
        if (keyType != Schema.Type.STRING) {
          throw new IllegalArgumentException(
            String.format("Only map keys of type string are supported. Field '%s' in path '%s' has keys of type '%s'.",
                          fieldName, path, keyType));
        }

        fieldSchema = valSchema;
        isNullable = fieldSchema.isNullable();
        fieldType = isNullable ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();

        Map map = (Map) val;
        val = map.get(key);

      } else {
        throw new IllegalArgumentException(
          String.format("Array syntax for nested field '%s' in path '%s' was specified, but '%s' is of type %s.",
                        fieldName, path, fieldName, fieldType));
      }
    }

    if (fields.hasNext()) {
      if (fieldType != Schema.Type.RECORD) {
        throw new IllegalArgumentException(
          String.format("Nested field '%s' in path '%s' must be of type '%s', but is '%s'.",
                        fieldName, path, Schema.Type.RECORD, fieldType));
      }

      return recursiveGet((StructuredRecord) val, path, fields);
    }
    return (T) val;
  }

}
