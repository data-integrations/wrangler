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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.AbstractSimpleStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * A Step to generate UUID.
 */
@Usage(
  directive = "generate-uuid",
  usage = "generate-uuid <column>",
  description = "Populates a column with a universally unique identifier."
)
public class GenerateUUID extends AbstractSimpleStep {
  private final String column;
  private final Random random;

  public GenerateUUID(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
    this.random = new Random();
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      UUID uuid = new UUID(random.nextLong(), random.nextLong());
      if (idx != -1) {
        record.setValue(idx, uuid.toString());
      } else {
        record.add(column, uuid.toString());
      }
    }
    return records;
  }

  @Override
  public Map<String, Set<String>> getColumnMap() {
    return ImmutableMap.<String, Set<String>>of(column, ImmutableSet.<String>of());
  }
}
