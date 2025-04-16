/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.directives.column;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A directive that creates a record from columns
 */
@Plugin(type = Directive.TYPE)
@Name(CreateRecord.NAME)
@Categories(categories = {"column"})
@Description("Creates Column of type Record .")
public class CreateRecord implements Directive, Lineage {
  public static final String NAME = "create-record";
  private String targetColumn;
  private String[] columns;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("target_column", TokenType.COLUMN_NAME);
    builder.define("columns", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    List<String> cols = ((ColumnNameList) args.value("columns")).value();
    targetColumn = args.value("target_column").value().toString();
    columns = cols.toArray(new String[0]);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();

    // Iterate through the rows.
    for (Row row : rows) {

      // We create a new Row with values from the columns specified.
      Row newRecord = new Row();
      for (String columnName : columns) {
        Object columnValue = row.getValue(columnName);
        // Only set value if column is set.
        if (columnValue != null) {
          newRecord.addOrSet(columnName, columnValue);
        }
      }
      // Add column to existing row
      Row newRow = new Row(row);
      newRow.addOrSet(targetColumn, newRecord);
      results.add(newRow);

    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Created column based on values in columns '%s''", Arrays.asList(columns))
      .relation(Many.columns(columns), targetColumn)
      .build();
  }
}
