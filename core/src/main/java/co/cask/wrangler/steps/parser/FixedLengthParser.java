/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Usage;

import java.util.ArrayList;
import java.util.List;

/**
 * A Fixed length Parser Stage for parsing the {@link Row} provided based on configuration.
 */
@Plugin(type = "udd")
@Name("parse-as-fixed-length")
@Usage("parse-as-fixed-length <column> <width>[,<width>*] [<padding-character>]")
@Description("Parses fixed-length records using the specified widths and padding-character.")
public final class FixedLengthParser extends AbstractDirective {
  private final int[] widths;
  private final String col;
  private final String padding;
  private final int recordLength;

  public FixedLengthParser(int lineno, String detail, String col, int[] widths, String padding) {
    super(lineno, detail);
    this.col = col;
    this.padding = padding;
    this.widths = widths;
    int sum = 0;
    for (int i = 0; i < widths.length; ++i) {
      sum += widths[i];
    }
    this.recordLength = sum;
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows     Input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled {@link Row}.
   * @throws DirectiveExecutionException In case of any issue this exception is thrown.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException, ErrorRecordException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          String data = (String) object;
          int length = data.length();
          // If the recordLength length doesn't match the string length.
          if (length < recordLength) {
            throw new ErrorRecordException(
              String.format("Fewer bytes than length of row specified - expected atleast %d bytes, found %s bytes.",
                            recordLength, length),
              2
            );
          }

          int index = 1;
          while ((index + recordLength - 1) <= length) {
            Row newRow = new Row(row);
            int recPosition = index;
            int colid = 1;
            for (int width : widths) {
              String val = data.substring(recPosition - 1, recPosition + width - 1);
              if (padding != null) {
                val = val.replaceAll(padding, "");
              }
              newRow.add(String.format("%s_%d", col, colid), val);
              recPosition += width;
              colid+=1;
            }
            results.add(newRow);
            index = (index + recordLength);
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", col)
          );
        }
      }
    }
    return results;
  }
}
