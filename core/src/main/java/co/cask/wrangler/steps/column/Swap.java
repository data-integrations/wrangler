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

import co.cask.wrangler.api.AbstractSimpleStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.api.i18n.Messages;
import co.cask.wrangler.api.i18n.MessagesFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A step for swapping the column names.
 */
@Usage(
  directive = "swap",
  usage = "swap <column1> <column2>",
  description = "Swap the column names."
)
public class Swap extends AbstractSimpleStep {
  private static final Messages MSG = MessagesFactory.getMessages();
  private final String column1;
  private final String column2;

  public Swap(int lineno, String directive, String column1, String column2) {
    super(lineno, directive);
    this.column1 = column1;
    this.column2 = column2;
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
      int sidx = record.find(column1);
      int didx = record.find(column2);

      if (sidx == -1) {
        throw new StepException(MSG.get("column.not.found", toString(), column1));
      }

      if (didx == -1) {
        throw new StepException(MSG.get("column.not.found", toString(), column2));
      }

      record.setColumn(sidx, column2);
      record.setColumn(didx, column1);
    }
    return records;
  }

  @Override
  public Map<String, Set<String>> getColumnMap() {
    return ImmutableMap.<String, Set<String>>of(column1, ImmutableSet.of(column2), column2, ImmutableSet.of(column1));
  }
}
