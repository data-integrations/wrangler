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

package io.cdap.directives.validation;

import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.directives.validation.conformers.Conformer;
import io.cdap.directives.validation.conformers.JSONConformer;
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
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A directive for erroring the processing if condition is set to true.
 */
@Plugin(type = Directive.TYPE)
@Name(ValidateStandard.NAME)
@Categories(categories = {"data-quality"})
@Description("Checks a column against a standard schema")
public class ValidateStandard implements Directive {

  public static final String NAME = "validate-standard";

  private static final String STANDARD_SPEC = "standard-spec";
  private static final String COLUMN = "column";

  private static Map<String, Conformer<JsonObject>> schemaToConformer = new HashMap<>();

  private String column;
  private String schema;

  private static Logger LOG = LoggerFactory.getLogger(ValidateStandard.class);


  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define(COLUMN, TokenType.COLUMN_NAME);
    builder.define(STANDARD_SPEC, TokenType.IDENTIFIER);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    column = ((ColumnName) args.value(COLUMN)).value();
    String spec = ((Identifier) args.value(STANDARD_SPEC)).value();

    if (spec.equals("")) {
      throw new DirectiveParseException("Nothing specified to validate against.");
    }

    schema = "schemas/" + spec + ".json";

    if (!schemaToConformer.containsKey(schema)) {
      InputStream schemaStream = ValidateStandard.class.getClassLoader()
          .getResourceAsStream(schema);

      if (schemaStream == null) {
        throw new DirectiveParseException("Unknown standard schema: " + spec);
      }

      JSONConformer validator = new JSONConformer(schemaStream);
      validator.initialize();

      schemaToConformer.put(schema, validator);
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
      throws DirectiveExecutionException, ErrorRowException, ReportErrorAndProceed {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof JsonObject) {
          Conformer<JsonObject> conformer = schemaToConformer.get(schema);
          if (conformer == null) {
            throw new DirectiveExecutionException("Directive was not initialized for schema " + schema);
          }

          conformer.checkConformance((JsonObject) object, row.getId());
        } else if (object != null) {
          throw new DirectiveExecutionException(
              String.format("Column %s is not a %s (it's %s)", column,
                  JsonObject.class.getName(), object.getClass().getName()));
        }
      }
    }
    return rows;
  }

  @Override
  public void destroy() {
    // no-op
  }
}
