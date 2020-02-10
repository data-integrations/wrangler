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
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for 'find-and-replace' transformations on the column.
 */
@Plugin(type = Directive.TYPE)
@Name(FindAndReplace.NAME)
@Categories(categories = { "transform"})
@Description("Finds and replaces text in column values using a sed-format expression.")
public class FindAndReplace implements Directive, Lineage {
  public static final String NAME = "find-and-replace";
  private String pattern;
  private List<String> columns;


  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME_LIST);
    builder.define("pattern", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.columns = ((ColumnNameList) args.value("column")).value();
    this.pattern = ((Text) args.value("pattern")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      for (String column : columns) {
        int idx = row.find(column);
        if (idx != -1) {
          Object v = row.getValue(idx);
          // Operates only on String types.
          try {
            if (v instanceof String) {
              String value = (String) v; // Safely converts to String.
              Unix4jCommandBuilder builder = Unix4j.echo(value).sed(pattern);
              if (builder.toExitValue() == 0) {
                row.setValue(idx, builder.toStringResult());
              }
            }
          } catch (Exception e) {
            // If there is any issue, we pass it on without any transformation.
          }
        }
        results.add(row);
      }
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    Mutation.Builder builder = Mutation.builder()
      .readable("Found and replaced '%s' using expression '%s'", columns, pattern);
    columns.forEach(column -> builder.relation(column, column));
    return builder.build();
  }
}

