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

package io.cdap.directives.column;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.SchemaResolutionContext;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Identifier;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.utils.ColumnConverter;

import java.math.RoundingMode;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Wrangler step for converting data type of column
 * Accepted types are: int, short, long, double, float, string, boolean and bytes
 * When decimal type is selected, can also specify the scale and rounding mode
 */
@Plugin(type = "directives")
@Name(SetType.NAME)
@Categories(categories = {"column"})
@Description("Converting data type of a column. Optional arguments scale and rounding-mode " +
  "are used only when type is decimal.")
public final class SetType implements Directive, Lineage {
  public static final String NAME = "set-type";

  private String col;
  private String type;
  private Integer scale;
  private RoundingMode roundingMode;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("type", TokenType.IDENTIFIER);
    builder.define("scale", TokenType.NUMERIC, Optional.TRUE);
    builder.define("rounding-mode", TokenType.TEXT, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    col = ((ColumnName) args.value("column")).value();
    type = ((Identifier) args.value("type")).value();
    if (type.equalsIgnoreCase("decimal")) {
      scale = args.contains("scale") ? ((Numeric) args.value("scale")).value().intValue() : null;
      if (scale == null && args.contains("rounding-mode")) {
        throw new DirectiveParseException("'rounding-mode' can only be specified when a 'scale' is set");
      }
      try {
        roundingMode = args.contains("rounding-mode") ?
          RoundingMode.valueOf(((Text) args.value("rounding-mode")).value()) :
          (scale == null ? RoundingMode.UNNECESSARY : RoundingMode.HALF_EVEN);
      } catch (IllegalArgumentException e) {
        throw new DirectiveParseException(String.format(
          "Specified rounding-mode '%s' is not a valid Java rounding mode", args.value("rounding-mode").value()), e);
      }
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      ColumnConverter.convertType(NAME, row, col, type, scale, roundingMode);
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Changed the column '%s' to type '%s'", col, type)
      .relation(col, col)
      .build();
  }

  @Override
  public Schema getOutputSchema(SchemaResolutionContext context) {
    String hi ="hi"; //MINU:: Branch to Force push from
    Schema inputSchema = context.getInputSchema();
    return Schema.recordOf(
      "outputSchema",
      inputSchema.getFields().stream()
        .map(
          field -> {
            try {
              return field.getName().equals(col) ?
                Schema.Field.of(col, ColumnConverter.getSchemaForType(type, scale)) : field;
            } catch (DirectiveParseException e) {
              throw new RuntimeException(e);
            }
          }
        )
        .collect(Collectors.toList())
    );
  }
}
