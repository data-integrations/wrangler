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

package co.cask.wrangler.steps.writer;

import co.cask.wrangler.api.AbstractUnboundedInputStep;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;
import java.util.Set;

/**
 * A step to write the record fields as CSV.
 */
@Usage(
  directive = "write-as-csv",
  usage = "write-as-csv <column>",
  description = "Writes the records files as well-formatted CSV"
)
public class WriteAsCSV extends AbstractUnboundedInputStep {

  private final String column;

  public WriteAsCSV(int lineno, String directive, String column) throws DirectiveParseException {
    super(lineno, directive);
    this.column = column;
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
      try {
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        try(Writer out = new BufferedWriter(new OutputStreamWriter(bOut))) {
          CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.DEFAULT);

          for (int i = 0; i < record.length(); ++i) {
            csvPrinter.print(record.getValue(i));
          }
          csvPrinter.flush();
          csvPrinter.close();
        } catch (Exception e) {
          bOut.close();
        }
        record.add(column, bOut.toString());
      } catch (IOException e) {
        throw new StepException(toString() + " : Failed to write CSV record. " + e.getMessage());
      }
    }
    return records;
  }

  @Override
  public Set<String> getBoundedOutputColumns() {
    return ImmutableSet.of(column);
  }

  @Override
  public Set<String> getOutputColumn(String inputColumn) {
    return ImmutableSet.of(column);
  }
}
