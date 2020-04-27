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
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for merging two columns and creates a third column.
 */
@Plugin(type = Directive.TYPE)
@Name(Merge.NAME)
@Categories(categories = { "column"})
@Description("Merges values from two columns using a separator into a new column.")
public class Merge implements Directive, Lineage {
  public static final String NAME = "merge";
  // Scope column1
  private String col1;

  // Scope column2
  private String col2;

  // Destination column name to be created.
  private String dest;

  // Delimiter to be used to merge column.
  private String delimiter;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column1", TokenType.COLUMN_NAME);
    builder.define("column2", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("separator", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.col1 = ((ColumnName) args.value("column1")).value();
    this.col2 = ((ColumnName) args.value("column2")).value();
    this.dest = ((ColumnName) args.value("destination")).value();
    this.delimiter = ((Text) args.value("separator")).value();
    delimiter = StringEscapeUtils.unescapeJava(delimiter);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx1 = row.find(col1);
      int idx2 = row.find(col2);
      if (idx1 != -1 && idx2 != -1) {
        StringBuilder builder = new StringBuilder();
        builder.append(row.getValue(idx1));
        builder.append(delimiter);
        builder.append(row.getValue(idx2));
        row.add(dest, builder.toString());
      }
      results.add(row);
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Merged column '%s' and '%s' using delimiter '%s' into column '%s'", col1, col2, delimiter, dest)
      .relation(Many.columns(col1, col2), Many.of(col1, col2, dest))
      .build();
  }
}
