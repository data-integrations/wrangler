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

package io.cdap.wrangler.schema;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.SchemaResolutionContext;
import io.cdap.wrangler.utils.RecordConvertorException;
import io.cdap.wrangler.utils.SchemaConverter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class can be used to generate the output schema for the output data of a directive. It maintains a map of
 * output fields present across all output rows after applying a directive. This map is used to generate the schema
 * if the directive does not return a custom output schema.
 */
public class DirectiveOutputSchemaGenerator {
  private final SchemaConverter schemaGenerator;
  private final Map<String, Object> outputFieldMap;
  private final Directive directive;

  public DirectiveOutputSchemaGenerator(Directive directive, SchemaConverter schemaGenerator) {
    this.directive = directive;
    this.schemaGenerator = schemaGenerator;
    outputFieldMap = new LinkedHashMap<>();
  }

  /**
   * Method to add new fields from the given output to the map of fieldName --> value maintained for schema generation.
   * A value is added to the map only if it is absent (or) if the existing value is null and given value is non-null
   * @param output list of output {@link Row}s after applying directive.
   */
  public void addNewOutputFields(List<Row> output) {
    for (Row row : output) {
      for (Pair<String, Object> field : row.getFields()) {
        String fieldName = field.getFirst();
        Object fieldValue = field.getSecond();
        if (outputFieldMap.containsKey(fieldName)) {
          // If existing value is null, override with this non-null value
          if (fieldValue != null && outputFieldMap.get(fieldName) == null) {
            outputFieldMap.put(fieldName, fieldValue);
          }
        } else {
          outputFieldMap.put(fieldName, fieldValue);
        }
      }
    }
  }

  /**
   * Method to get the output schema of the directive. Returns a generated schema based on maintained map of fields
   * only if directive does not return a custom output schema.
   * @param context input {@link Schema} of the data before applying the directive
   * @return {@link Schema} corresponding to the output data
   */
  public Schema getDirectiveOutputSchema(SchemaResolutionContext context) throws RecordConvertorException {
    Schema directiveOutputSchema = directive.getOutputSchema(context);
    return directiveOutputSchema != null ? directiveOutputSchema :
      generateDirectiveOutputSchema(context.getInputSchema());
  }

  // Given the schema from previous step and output of current directive, generates the directive output schema.
  private Schema generateDirectiveOutputSchema(Schema inputSchema)
    throws RecordConvertorException {
    List<Schema.Field> outputFields = new ArrayList<>();
    for (Map.Entry<String, Object> field : outputFieldMap.entrySet()) {
      String fieldName = field.getKey();
      Object fieldValue = field.getValue();

      Schema existing = inputSchema.getField(fieldName) != null ? inputSchema.getField(fieldName).getSchema() : null;
      Schema generated = fieldValue != null && !isValidSchemaForValue(existing, fieldValue) ?
        schemaGenerator.getSchema(fieldValue, fieldName) : null;

      if (generated != null) {
        outputFields.add(Schema.Field.of(fieldName, generated));
      } else if (existing != null) {
        if (!existing.getType().equals(Schema.Type.NULL) && !existing.isNullable()) {
          existing = Schema.nullableOf(existing);
        }
        outputFields.add(Schema.Field.of(fieldName, existing));
      } else {
        outputFields.add(Schema.Field.of(fieldName, Schema.of(Schema.Type.NULL)));
      }
    }
    if (outputFields.isEmpty()) {
      return null;
    }
    return Schema.recordOf("output", outputFields);
  }

  // Checks whether the provided input schema is of valid type for given object
  private boolean isValidSchemaForValue(@Nullable Schema schema, Object value) throws RecordConvertorException {
    if (schema == null) {
      return false;
    }
    Schema generated = schemaGenerator.getSchema(value, "temp_field_name");
    generated = generated.isNullable() ? generated.getNonNullable() : generated;
    schema = schema.isNullable() ? schema.getNonNullable() : schema;
    return generated.getLogicalType() == schema.getLogicalType() && generated.getType() == schema.getType();
  }
}
