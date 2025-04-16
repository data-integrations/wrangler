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
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A directive extracts regex groups into separate columns.
 */
@Plugin(type = Directive.TYPE)
@Name(ExtractRegexGroups.NAME)
@Categories(categories = { "transform"})
@Description("Extracts data from a regex group into its own column.")
public class ExtractRegexGroups implements Directive, Lineage {
  public static final String NAME = "extract-regex-groups";
  private String column;
  private String regex;
  private Pattern pattern;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("regex", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.regex = ((Text) args.value("regex")).value();
    pattern = Pattern.compile(regex);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object value = row.getValue(idx);
        if (value != null && value instanceof String) {
          Matcher matcher = pattern.matcher((String) value);
          int count = 1;
          while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
              row.add(String.format("%s_%d_%d", column, count, i), matcher.group(i));
            }
            count++;
          }
        }
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Split column '%s' based on expression '%s' ", column, regex)
      .all(Many.of(column), Many.of(column))
      .build();
  }
}

