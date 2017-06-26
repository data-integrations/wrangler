/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts regex groups into separate columns.
 */
@Plugin(type = "udd")
@Name("extract-regex-groups")
@Usage("extract-regex-groups <column> <regex-with-groups>")
@Description("Extracts data from a regex group into its own column.")
public class ExtractRegexGroups extends AbstractDirective {
  private final String column;
  private final String regex;
  private final Pattern pattern;

  public ExtractRegexGroups(int lineno, String directive, String column, String regex) {
    super(lineno, directive);
    this.column = column;
    this.regex = regex;
    pattern = Pattern.compile(regex);

  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object value = row.getValue(idx);
        if (value != null && value instanceof String) {
          Matcher matcher = pattern.matcher((String) value);
          int count = 1;
          while (matcher.find()) {
            for(int i = 1; i <= matcher.groupCount(); i++) {
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

