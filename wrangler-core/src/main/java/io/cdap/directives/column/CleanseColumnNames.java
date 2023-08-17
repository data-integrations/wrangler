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
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.SchemaResolutionContext;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A directive for cleanses columns names.
 *
 * <p>
 *   <ul>
 *     <li>Lowercases the column name</li>
 *     <li>Trims space</li>
 *     <li>Replace characters other than [A-Z][a-z][_] with empty string.</li>
 *   </ul>
 * </p>
 */
@Plugin(type = Directive.TYPE)
@Name(CleanseColumnNames.NAME)
@Categories(categories = { "column"})
@Description("Sanatizes column names: trims, lowercases, and replaces all but [A-Z][a-z][0-9]_." +
  "with an underscore '_'.")
public final class CleanseColumnNames implements Directive, Lineage {
  public static final String NAME = "cleanse-column-names";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    // no-op.
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      for (int i = 0; i < row.width(); ++i) {
        String column = row.getColumn(i);
        row.setColumn(i, cleanseColumnName(column));
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Sanitized all column names: trimmed sides, lower-cased, " +
                  "and replaced all but [A-Z][a-z][0-9]_ characters")
      .all(Many.of())
      .build();
  }

  @Override
  public Schema getOutputSchema(SchemaResolutionContext context) {
    Schema inputSchema = context.getInputSchema();
    return Schema.recordOf(
      "outputSchema",
      inputSchema.getFields().stream()
        .map(
          field -> Schema.Field.of(cleanseColumnName(field.getName()), field.getSchema())
        )
        .collect(Collectors.toList())
    );
  }

  private String cleanseColumnName(String columnName) {
    // Trims
    columnName = columnName.trim();
    // Lower case columns
    columnName = columnName.toLowerCase();
    // Filtering unwanted characters
    columnName = columnName.replaceAll("[^a-zA-Z0-9_]", "_");
    return columnName;
  }
}
