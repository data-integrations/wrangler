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

package io.cdap.directives.transformation;

import com.google.common.base.Strings;
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
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for splitting a col into two additional columns based on a delimiter.
 */
@Plugin(type = Directive.TYPE)
@Name(Split.NAME)
@Categories(categories = { "readable"})
@Description("Use 'split-to-columns' or 'split-to-rows'.")
@Deprecated
public class Split implements Directive {
  public static final String NAME = "split";
  // Name of the column to be split
  private String col;

  private String delimiter;

  // Destination column names
  private String firstColumnName, secondColumnName;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("delimiter", TokenType.TEXT);
    builder.define("column1", TokenType.COLUMN_NAME);
    builder.define("column2", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.col = ((ColumnName) args.value("source")).value();
    this.delimiter = ((Text) args.value("delimiter")).value();
    this.firstColumnName = ((ColumnName) args.value("column1")).value();
    this.secondColumnName = ((ColumnName) args.value("column2")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        String val = (String) row.getValue(idx);
        if (val != null) {
          String[] parts = val.split(delimiter, 2);
          if (Strings.isNullOrEmpty(parts[0])) {
            row.add(firstColumnName, parts[1]);
            row.add(secondColumnName, null);
          } else {
            row.add(firstColumnName, parts[0]);
            row.add(secondColumnName, parts[1]);
          }
        } else {
          row.add(firstColumnName, null);
          row.add(secondColumnName, null);
        }
      } else {
        throw new DirectiveExecutionException(
          col + " is not of type string. Please check the wrangle configuration."
        );
      }
      results.add(row);
    }
    return results;
  }
}
