/*
 *  Copyright © 2019 Google Inc.
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

package io.cdap.directives.validation.conformers;

import com.google.gson.JsonObject;
import io.cdap.directives.validation.ConformanceException;
import io.cdap.directives.validation.ValidationIssue;
import java.io.InputStream;
import java.util.Arrays;
import java.util.UUID;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.Validator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Validates using JSON Validator.
 */
public class JSONConformer implements Conformer<JsonObject> {

  private final InputStream schemaStream;
  private Schema schema;

  public JSONConformer(InputStream stream) {
    this.schemaStream = stream;
  }

  @Override
  public void initialize() {
    JSONObject schemaJson = new JSONObject(new JSONTokener(schemaStream));

    schema = SchemaLoader
        .builder()
        .schemaJson(schemaJson)
        .build()
        .load()
        .build();
  }

  @Override
  public void checkConformance(JsonObject value, UUID id) throws ConformanceException {
    JSONObject orgJsonObject = new JSONObject(new JSONTokener(value.toString()));

    try {
      Validator validator = Validator.builder()
          .failEarly()
          .build();

      validator.performValidation(schema, orgJsonObject);
    } catch (ValidationException e) {
      throw convertValidationException(e, id);
    }
  }

  private static ConformanceException convertValidationException(ValidationException e,
      UUID id) {
    ValidationException[] nonTrivialCauses = e.getCausingExceptions()
        .stream()
        .filter(c -> !c.getPointerToViolation().equals("#"))
        .toArray(ValidationException[]::new);

    if (nonTrivialCauses.length > 0) {
      return new ConformanceException(
          Arrays
              .stream(nonTrivialCauses)
              .map(
                  ntc -> new ValidationIssue(ntc.getSchemaLocation(), ntc.getPointerToViolation(),
                      ntc.getMessage(), id))
              .toArray(ValidationIssue[]::new)
      );
    }

    return new ConformanceException(
        new ValidationIssue(e.getSchemaLocation(), e.getPointerToViolation(),
            e.toJSON().toString(), id));
  }
}
