/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A Fixed length Parser Stage for parsing the {@link Record} provided based on configuration.
 */
@Usage(directive = "parse-as-fixed-length", usage = "parse-as-fixed-length <source> <width,width,...>")
public final class FixedLengthParser extends AbstractStep {
  private final int[] widths;
  private final String col;
  private final String padding;

  public FixedLengthParser(int lineno, String detail, String col, int[] widths, String padding) {
    super(lineno, detail);
    this.col = col;
    this.padding = padding;
    this.widths = widths;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records     Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   * @throws StepException In case of any issue this exception is thrown.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    List<Record> results = new ArrayList<>();
    for (Record record : records) {
      int idx = record.find(col);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object instanceof String) {
          String value = (String) object;
          int offset = 0;
          for (int i = 0; i < widths.length; ++i) {
            String val = value.substring(offset, offset + widths[i]);
            val = val.replaceAll(padding, "");
            record.add(String.format("%s_%d", col, i+1), val);
            offset = offset + widths[i];
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", col)
          );
        }
      }
      results.add(record);
    }
    return results;
  }
}