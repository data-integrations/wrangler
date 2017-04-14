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

package co.cask.wrangler.steps.row;

import co.cask.wrangler.api.AbstractSimpleStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Filters records if they don't have all the columns specified or they have null values or combination.
 */
@Usage(
  directive = "filter-rows-on",
  usage="filter-rows-on empty-or-null-columns <column>[,<column>]*",
  description = "Filters row that have empty or null columns."
)
public class RecordMissingOrNullFilter extends AbstractSimpleStep {
  private final String[] columns;

  public RecordMissingOrNullFilter(int lineno, String directive, String[] columns) {
    super(lineno, directive);
    this.columns = columns;
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
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      boolean missingOrNull = true;
      for (String column : columns) {
        int idx = record.find(column.trim());
        if (idx != -1) {
          Object value = record.getValue(idx);
          if (value != null) {
            missingOrNull = false;
          }
        }
      }
      if (!missingOrNull) {
        results.add(record);
      }
    }
    return results;
  }

  @Override
  public Map<String, Set<String>> getColumnMap() {
    ImmutableMap.Builder<String, Set<String>> builder = ImmutableMap.builder();

    for (String column : columns) {
      builder.put(column, ImmutableSet.of(column));
    }

    return builder.build();
  }
}
