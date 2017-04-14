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

package co.cask.wrangler.steps.column;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.AbstractKeepStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A step that implements the opposite of {@link Drop} columns.
 */
@Usage(
  directive = "keep",
  usage = "keep <column>[,<column>]*",
  description = "Keep the columns specified and drop the rest."
)
public class Keep extends AbstractKeepStep {

  private final Set<String> keep = new HashSet<>();

  public Keep(int lineno, String directive, String[] columns) {
    super(lineno, directive);
    for (String column : columns) {
      keep.add(column.trim());
    }
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
      int idx = 0;
      for (KeyValue<String, Object> v : record.getFields()) {
        if (!keep.contains(v.getKey())) {
          record.remove(idx);
        } else {
          ++idx;
        }
      }
    }
    return records;
  }

  @Override
  public Set<String> getKeptColumns() {
    return ImmutableSet.copyOf(keep);
  }
}
