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
import io.cdap.wrangler.api.Pair;
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
 * A directive for splitting a RowType ("Record") Column into multiple Columns.
 *
 * The directive takes multiple columns of type "Row" as brings all its elements up a level.
 * Topmost elements will be set under new columns prefixed with the name of the parent column
 * followed by an underscore. Existing columns with the same name will be replaced by the new values.
 *
 * E.G
 *
 * Input:
 * |  A |
 * {B: 1, C: 2}
 *
 * Directive
 * flatten-record :A
 *
 * Output:
 * | A          | A_B | A_C |
 * {B: 1, C:2}  | 1   |  2 |
 *
 */
@Plugin(type = Directive.TYPE)
@Name(FlattenRecord.NAME)
@Categories(categories = { "column"})
@Description("Flattens a record into individual columns.")
public class FlattenRecord implements Directive {
  public static final String NAME = "flatten-record";
  // Column on which to apply mask.
  private String[] columns;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("columns", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    List<String> cols = ((ColumnNameList) args.value("columns")).value();
    columns = new String[cols.size()];
    columns = cols.toArray(columns);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();

    for (Row row : rows) {
      for (String column: columns) {
        int idx = row.find(column);
        if (idx == -1) {
          continue;
        }
        Object object = row.getValue(idx);

        if (object == null) {
          continue;
        }

        if (!(object instanceof Row)) {
          throw new DirectiveExecutionException(
            NAME,
            String.format("Column '%s' has invalid type '%s'. It should be of type 'Record'.",
                          column, object.getClass().getSimpleName()));
        }

        Row rowColumns = (Row) object;

        String columnName;
        for (Pair<String, Object> field : rowColumns.getFields()) {
          columnName = String.format("%s_%s", column, field.getFirst());
          row.addOrSet(columnName, field.getSecond());
        }
      }
      results.add(row);
    }
    return results;
  }
}
