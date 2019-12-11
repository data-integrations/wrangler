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

package io.cdap.directives.validation;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.directives.validation.conformers.Conformer;
import io.cdap.directives.validation.conformers.JsonConformer;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.ReportErrorAndProceed;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.utils.Manifest;
import io.cdap.wrangler.utils.Manifest.Standard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * A directive for validating data against a number of built-in data models.
 */
@Plugin(type = Directive.TYPE)
@Name(ValidateStandard.NAME)
@Categories(categories = {"data-quality"})
@Description("Checks a column against a standard schema")
public class ValidateStandard implements Directive {

  public static final String NAME = "validate-standard";
  static final String SCHEMAS_RESOURCE_PATH = "schemas/";
  static final String MANIFEST_PATH = SCHEMAS_RESOURCE_PATH + "manifest.json";
  static final Map<String, Conformer.Factory<JsonObject>> FORMAT_TO_FACTORY = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(ValidateStandard.class);
  private static final String STANDARD_SPEC = "standard-spec";
  private static final String COLUMN = "column";
  private static final Map<String, Conformer<JsonObject>> schemaToConformer = new HashMap<>();
  private static Manifest standardsManifest = null;

  static {
    FORMAT_TO_FACTORY.put(JsonConformer.SCHEMA_FORMAT, new JsonConformer.Factory());

    try {
      standardsManifest = getManifest();
    } catch (IOException e) {
      LOG.error("Unable to read standards manifest", e);
    }
  }

  private String column;
  private String schema;

  private static Manifest getManifest() throws IOException {
    InputStream resourceStream =
      ValidateStandard.class.getClassLoader().getResourceAsStream(ValidateStandard.MANIFEST_PATH);

    if (resourceStream == null) {
      throw new IOException(
        String.format("Can't read/find resource %s", ValidateStandard.MANIFEST_PATH));
    }

    InputStream manifestStream = readResource(ValidateStandard.MANIFEST_PATH);
    return new Gson().getAdapter(Manifest.class).fromJson(new InputStreamReader(manifestStream));
  }

  private static InputStream readResource(String name) {
    InputStream resourceStream = ValidateStandard.class.getClassLoader().getResourceAsStream(name);

    if (resourceStream == null) {
      throw new IllegalArgumentException("Can't read/find resource " + name);
    }

    return resourceStream;
  }

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(COLUMN, TokenType.COLUMN_NAME);
    builder.define(
      STANDARD_SPEC,
      TokenType.IDENTIFIER,
      String.format(
        "[one of: %s]", String.join(", ", standardsManifest.getStandards().keySet())));

    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    column = ((ColumnName) args.value(COLUMN)).value();
    String spec = ((Identifier) args.value(STANDARD_SPEC)).value();

    if (spec.equals("")) {
      throw new DirectiveParseException("No standard specified to validate against");
    }
    if (standardsManifest == null) {
      throw new DirectiveParseException(
        "Standards manifest was not loaded. Please check logs for information");
    }

    Map<String, Standard> availableSpecs = standardsManifest.getStandards();
    if (!availableSpecs.containsKey(spec)) {
      throw new DirectiveParseException(
        String.format(
          "Unknown standard %s. Known values are %s",
          spec, String.join(", ", standardsManifest.getStandards().keySet())));
    }

    Standard standard = availableSpecs.get(spec);
    schema =
      Paths.get(SCHEMAS_RESOURCE_PATH, String.format("%s.%s", spec, standard.getFormat()))
        .toString();

    if (!schemaToConformer.containsKey(schema)) {
      if (!FORMAT_TO_FACTORY.containsKey(standard.getFormat())) {
        throw new DirectiveParseException(String.format("No validator for format %s", standard.getFormat()));
      }

      try {
        Conformer<JsonObject> conformer = FORMAT_TO_FACTORY.get(standard.getFormat())
          .setSchemaStreamSupplier(() -> readResource(schema))
          .build();
        conformer.initialize();
        schemaToConformer.put(schema, conformer);
      } catch (IOException e) {
        throw new DirectiveParseException(String.format("Unable to read standard schema: %s", e.getMessage()), e);
      }
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
    for (Row row : rows) {
      int idx = row.find(column);

      if (idx < 0) {
        continue;
      }

      Object object = row.getValue(idx);
      if (object == null) {
        continue;
      }

      if (!(object instanceof JsonObject)) {
        throw new DirectiveExecutionException(
          String.format(
            "Column %s is not a %s (it's %s)",
            column, JsonObject.class.getName(), object.getClass().getName()));
      }

      Conformer<JsonObject> conformer = schemaToConformer.get(schema);
      if (conformer == null) {
        throw new DirectiveExecutionException("Directive was not initialized for schema " + schema);
      }

      List<ConformanceIssue> conformanceIssues =
        conformer.checkConformance((JsonObject) object);
      if (conformanceIssues.size() > 0) {
        throw new ErrorRowException(
          conformanceIssues.stream()
            .map(ConformanceIssue::toString)
            .collect(Collectors.joining("; ")),
          1, true);
      }
    }
    return rows;
  }

  @Override
  public void destroy() {
    // no-op
  }
}
