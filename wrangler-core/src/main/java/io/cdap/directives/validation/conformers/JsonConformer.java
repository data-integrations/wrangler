/*
 *  Copyright © 2019 Cask Data, Inc.
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
import io.cdap.directives.validation.ConformanceIssue;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.Validator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates using JSON Validator.
 */
public class JsonConformer implements Conformer<JsonObject> {

  public static final String SCHEMA_FORMAT = "json";

  private final Supplier<InputStream> schemaStream;
  private Schema schema;

  private JsonConformer(Supplier<InputStream> stream) {
    this.schemaStream = stream;
  }

  static List<ConformanceIssue> convertValidationException(ValidationException e) {
    ValidationException[] nonTrivialCauses =
      e.getCausingExceptions().stream()
        .filter(c -> !c.getPointerToViolation().equals("#"))
        .toArray(ValidationException[]::new);

    if (nonTrivialCauses.length > 0) {
      return Arrays.stream(nonTrivialCauses)
        .flatMap(ntc -> getLeafExceptions(null, ntc))
        .distinct()
        .collect(Collectors.toList());
    }

    return Collections.singletonList(
      new ConformanceIssue(
        e.getSchemaLocation(), e.getPointerToViolation(), e.toJSON().toString()));
  }

  private static Stream<ConformanceIssue> getLeafExceptions(String schemaPath, ValidationException ve) {
    String newSchemaPath = (schemaPath != null && !schemaPath.isEmpty() ? schemaPath + " -> " : "")
      + ve.getSchemaLocation();

    if (ve.getCausingExceptions().size() == 0) {
      return Stream.of(new ConformanceIssue(newSchemaPath, ve.getPointerToViolation(), ve.getMessage()));
    }

    return ve.getCausingExceptions().stream().flatMap(c -> getLeafExceptions(newSchemaPath, c));
  }

  @Override
  public void initialize() throws IOException {

    try (
      InputStream stream = schemaStream.get();
      InputStreamReader reader = new InputStreamReader(stream)
    ) {
      JSONObject schemaJson = new JSONObject(new JSONTokener(reader));
      schema = SchemaLoader.builder().schemaJson(schemaJson).build().load().build();
    }
  }

  @Override
  public List<ConformanceIssue> checkConformance(JsonObject value) {
    JSONObject orgJsonObject = new JSONObject(new JSONTokener(value.toString()));

    try {
      Validator validator = Validator.builder().failEarly().build();
      validator.performValidation(schema, orgJsonObject);
    } catch (ValidationException e) {
      return convertValidationException(e);
    }

    return Collections.emptyList();
  }

  /**
   * Factory for JsonConformer. Requires only a schema.
   */
  public static class Factory implements Conformer.Factory<JsonObject> {

    private Supplier<InputStream> stream;

    public Factory setSchemaStreamSupplier(Supplier<InputStream> schemaStream) {
      this.stream = schemaStream;
      return this;
    }

    public JsonConformer build() {
      return new JsonConformer(stream);
    }
  }
}
