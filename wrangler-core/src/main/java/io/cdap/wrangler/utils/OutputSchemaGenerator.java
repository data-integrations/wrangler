/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * JAVADOC
 */
public final class OutputSchemaGenerator {
  private static final String TEMP_SCHEMA_FIELD_NAME = "temporarySchemaField";

  private static final SchemaConverter SCHEMA_GENERATOR = new SchemaConverter();

  /**
   * Method to generate the output schema for the given output rows
   * @param inputSchema {@link Schema} of the data before transformation
   * @param output rows of data after transformation
   * @return generated {@link Schema} of the output data
   * @throws RecordConvertorException
   */
  public static Schema generateOutputSchema(Schema inputSchema, Row output) throws RecordConvertorException {
    List<Schema.Field> outputFields = new LinkedList<>();
    for (Pair<String, Object> rowField : output.getFields()) {
      String fieldName = rowField.getFirst();
      Object fieldValue = rowField.getSecond();

      Schema existing = inputSchema.getField(fieldName) != null ? inputSchema.getField(fieldName).getSchema() : null;
      Schema generated = fieldValue == null ? Schema.of(Schema.Type.NULL) :
        (!isValidSchemaForValue(existing, fieldValue) ? SCHEMA_GENERATOR.getSchema(fieldValue, fieldName) : null);

      if (generated != null) {
        outputFields.add(Schema.Field.of(fieldName, generated));
      } else if (existing != null) {
        outputFields.add(Schema.Field.of(fieldName, existing));
      }
    }
    return Schema.recordOf("output", outputFields);
  }

  /**
   *
   * @param first
   * @param second
   * @return
   */
  public static Schema getSchemaUnion(@Nullable Schema first, @Nullable Schema second) {
    if (first == null) {
      return second;
    }
    if (second == null) {
      return first;
    }
    Map<String, Schema> fieldMap = new LinkedHashMap<>();
    for (Schema.Field field : first.getFields()) {
      fieldMap.put(field.getName(), field.getSchema());
    }
    for (Schema.Field field : second.getFields()) {
      if (field.getSchema().getType().equals(Schema.Type.NULL) && fieldMap.containsKey(field.getName())) {
        continue;
      }
      fieldMap.put(field.getName(), field.getSchema());
    }
    List<Schema.Field> outputFields = fieldMap.entrySet().stream()
      .map(e -> Schema.Field.of(e.getKey(), e.getValue()))
      .collect(Collectors.toList());
    return Schema.recordOf(TEMP_SCHEMA_FIELD_NAME, outputFields);
  }

  // Checks whether the provided input schema is of valid type for given object
  private static boolean isValidSchemaForValue(@Nullable Schema schema, Object value) throws RecordConvertorException {
    if (schema == null) {
      return false;
    }
    Schema generated = SCHEMA_GENERATOR.getSchema(value, TEMP_SCHEMA_FIELD_NAME);
    generated = generated.isNullable() ? generated.getNonNullable() : generated;
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    return generated.getType().equals(schema.getType());
  }
}
