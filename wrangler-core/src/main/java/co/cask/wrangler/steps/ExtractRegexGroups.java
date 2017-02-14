/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by nitin on 2/13/17.
 */
@Usage(directive = "extract-regex-groups", usage = "extract-regex-groups <column> <regex-with-groups>")
public class ExtractRegexGroups extends AbstractStep {
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
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object value = record.getValue(idx);
        if (value != null && value instanceof String) {
          Matcher matcher = pattern.matcher((String) value);
          if(matcher.matches()) {
            for(int i = 0; i < matcher.groupCount(); i++) {
              record.add(String.format("%s_%d", column, i), matcher.group(i));
            }
          }
        }
      }
    }
    return records;
  }
}
