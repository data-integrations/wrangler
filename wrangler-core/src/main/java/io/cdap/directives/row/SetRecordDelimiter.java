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

package io.cdap.directives.row;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Numeric;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;
import java.util.List;

/**
 * A directive for parsing a string into record using the record delimiter.
 */
@Plugin(type = Directive.TYPE)
@Name(SetRecordDelimiter.NAME)
@Categories(categories = { "row" })
@Description("Sets the record delimiter.")
public class SetRecordDelimiter implements Directive, Lineage {
  public static final String NAME = "set-record-delim";
  private String column;
  private String delimiter;
  private int limit;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("delimiter", TokenType.TEXT);
    builder.define("limit", TokenType.NUMERIC, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    column = ((ColumnName) args.value("column")).value();
    delimiter = ((Text) args.value("delimiter")).value();
    if (args.contains("limit")) {
      Numeric numeric = args.value("limit");
      limit = numeric.value().intValue();
    } else {
      limit = Integer.MAX_VALUE;
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException, ErrorRowException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = row.getValue(idx);
      if (object instanceof String) {
        String body = (String) object;
        String[] lines = body.split(delimiter);
        int i = 0;
        for (String line : lines) {
          if (i > limit) {
            break;
          }
          results.add(new Row(column, line));
          i++;
        }
      }
    }
    return results;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Split value in column '%s' into multiple records using delimiter '%s'", column, delimiter)
      .relation(column, column)
      .build();
  }
}
