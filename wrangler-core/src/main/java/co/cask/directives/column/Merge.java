/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.column;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.lineage.MutationDefinition;
import co.cask.wrangler.api.lineage.MutationType;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for merging two columns and creates a third column.
 */
@Plugin(type = Directive.Type)
@Name(Merge.NAME)
@Categories(categories = { "column"})
@Description("Merges values from two columns using a separator into a new column.")
public class Merge implements Directive {
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
        row.add(dest, row.getValue(idx1) + delimiter + row.getValue(idx2));
      }
      results.add(row);
    }
    return results;
  }

  @Override
  public MutationDefinition lineage() {
    MutationDefinition.Builder builder = new MutationDefinition.Builder(NAME);
    builder.addMutation(col1, MutationType.READ);
    builder.addMutation(col2, MutationType.READ);
    builder.addMutation(dest, MutationType.ADD);
    return builder.build();
  }
}
