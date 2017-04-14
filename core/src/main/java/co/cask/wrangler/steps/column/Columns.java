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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.KeepStep;
import co.cask.wrangler.api.OptimizerGraphBuilder;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.UnboundedInputStep;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A Wrangle step for setting the columns obtained from wrangling.
 *
 * This step will create a copy of the input {@link Record} and clears
 * all previous column names and add new column names.
 */
@Usage(
  directive = "set columns",
  usage = "set columns <column,column,...>",
  description = "Sets the column names for CSV parsed records."
)
public class Columns extends AbstractStep implements UnboundedInputStep<Record, Record, String>,
  KeepStep<Record, Record, String> {

  // Name of the columns represented in a {@link Record}
  private List<String> columns = new ArrayList<>();

  // Replaces the input {@link Record} column names.
  private boolean replaceColumnNames;

  public Columns(int lineno, String detail, List<String> columns) {
    this(lineno, detail, columns, true);
  }

  public Columns(int lineno, String detail, List<String> columns, boolean replaceColumnNames) {
    super(lineno, detail);
    this.replaceColumnNames = replaceColumnNames;
    for (String column : columns) {
      column = column.replaceAll("\"|'", "");
      column = column.trim();
      this.columns.add(column);
    }
  }

  /**
   * Sets the new column names for the {@link Record}.
   *
   * @param records Input {@link Record} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return A newly transformed {@link Record}.
   * @throws StepException
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = 0;
      for (String name : columns) {
        if (idx < record.length()) {
          record.setColumn(idx, name);
        }
        idx++;
      }
    }
    return records;
  }

  @Override
  public void acceptOptimizerGraphBuilder(OptimizerGraphBuilder<Record, Record, String> optimizerGraphBuilder) {
    optimizerGraphBuilder.buildGraph((UnboundedInputStep<Record, Record, String>) this);
    if (replaceColumnNames) {
      optimizerGraphBuilder.buildGraph((KeepStep<Record, Record, String>) this);
    }
  }

  @Override
  public Set<String> getKeptColumns() {
    return ImmutableSet.copyOf(columns);
  }

  @Override
  public Set<String> getBoundedOutputColumns() {
    return ImmutableSet.copyOf(columns);
  }

  @Override
  public Set<String> getOutputColumn(String inputColumn) {
    return ImmutableSet.copyOf(columns);
  }
}

