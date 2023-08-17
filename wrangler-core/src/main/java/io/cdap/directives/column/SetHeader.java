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
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for setting the columns obtained from wrangling.
 *
 * This step will create a copy of the input {@link Row} and clears
 * all previous column names and add new column names.
 */
@Plugin(type = "directives")
@Name(SetHeader.NAME)
@Categories(categories = { "column"})
@Description("Sets the header of columns, in the order they are specified.")
public class SetHeader implements Directive, Lineage {
  public static final String NAME = "set-headers";
  // Name of the columns represented in a {@link Row}
  private List<String> columns = new ArrayList<>();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    columns = ((ColumnNameList) args.value("column")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = 0;
      for (String name : columns) {
        if (idx < row.width()) {
          row.setColumn(idx, name.trim());
        }
        idx++;
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Set the new header as columns '%s'", columns)
      .generate(Many.of(columns))
      .build();
  }

  @Override
  public Schema getOutputSchema(SchemaResolutionContext context) {
    List<Schema.Field> inputFields = context.getInputSchema().getFields();
    List<Schema.Field> outputFields = new ArrayList<>();
    for (int i = 0; i < columns.size() && i < inputFields.size(); i++) {
      outputFields.add(Schema.Field.of(columns.get(i).trim(), inputFields.get(i).getSchema()));
    }
    // Leftover columns (not renamed)
    for (int i = columns.size(); i < inputFields.size(); i++) {
      outputFields.add(inputFields.get(i));
    }
    return Schema.recordOf("outputSchema", outputFields);
  }
}
