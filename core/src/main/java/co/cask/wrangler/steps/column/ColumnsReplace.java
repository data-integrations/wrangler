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

import co.cask.wrangler.api.AbstractUnboundedInputOutputStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;
import org.unix4j.Unix4j;
import org.unix4j.builder.Unix4jCommandBuilder;

import java.util.List;
import java.util.Set;

/**
 * Applies a sed expression on the column names.
 *
 * This directive helps clearing out the columns names to make it more readable.
 */
@Usage(
  directive = "columns-replace",
  usage = "columns-replace <sed-expression>",
  description = "Modify column names using sed format."
)
public class ColumnsReplace extends AbstractUnboundedInputOutputStep {
  private final String sed;

  public ColumnsReplace(int lineno, String detail, String sed) {
    super(lineno, detail);
    this.sed = sed.trim();
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
      for (int i = 0; i < record.length(); ++i) {
        String name = record.getColumn(i);
        try {
          Unix4jCommandBuilder builder = Unix4j.echo(name).sed(sed);
          record.setColumn(i, builder.toStringResult());
        } catch (IllegalArgumentException e) {
          throw new StepException(
            String.format(toString() + " : " + e.getMessage())
          );
        }
      }
    }
    return records;
  }

  @Override
  public Set<String> getInputColumns(String outputColumn) {
    return ImmutableSet.of(outputColumn);
  }
}

