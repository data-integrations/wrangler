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

package co.cask.directives.transformation;

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
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

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
public class ExtractRegexGroups implements Directive {
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
}

