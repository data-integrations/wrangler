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
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class <code>Keep</code> implements a directive that
 * opposite of {@link Drop} columns. Instead of dropping the
 * columns specified, it keeps only those columns that are
 * specified.
 */
@Plugin(type = Directive.TYPE)
@Name("keep")
@Categories(categories = { "column"})
@Description("Keeps the specified columns and drops all others.")
public class Keep implements Directive, Lineage {
  public static final String NAME = "keep";
  private final Set<String> keep = new HashSet<>();

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    ColumnNameList cols = args.value("column");
    for (String col : cols.value()) {
      keep.add(col);
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = 0;
      for (Pair<String, Object> v : row.getFields()) {
        if (!keep.contains(v.getFirst())) {
          row.remove(idx);
        } else {
          ++idx;
        }
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    Mutation.Builder builder = Mutation.builder()
                                  .readable("Removed all columns except '%s'", keep);
    keep.forEach(column -> builder.relation(column, column));
    return builder.build();
  }
}
