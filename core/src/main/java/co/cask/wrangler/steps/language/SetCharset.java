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

package co.cask.wrangler.steps.language;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Sets the character set encoding on the column.
 *
 * This directive will convert the data from {@link Byte[]} or {@link ByteBuffer}
 * to {@link String}. This conversion is through the character set encoding.
 */
@Usage(
  directive = "set-charset",
  usage = "set-charset <column> <charset>",
  description = "Sets the character set decoding to UTF-8."
)
public class SetCharset extends AbstractStep {
  private final String column;
  private final String charset;

  public SetCharset(int lineno, String detail, String column, String charset) {
    super(lineno, detail);
    this.column = column;
    this.charset = charset;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException,
    ErrorRecordException {

    // Iterate through all the records.
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = record.getValue(idx);
      if (object == null) {
        continue;
      }

      // Convert from byte[] or ByteBuffer into right ByteBuffer.
      ByteBuffer buffer;
      if (object instanceof byte[]) {
        buffer = ByteBuffer.wrap((byte[]) object);
      } else if (object instanceof ByteBuffer) {
        buffer = (ByteBuffer) object;
      } else {
        throw new StepException(
          String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                        object != null ? object.getClass().getName() : "null", column)

        );
      }

      try {
        CharBuffer result = Charset.forName(charset).decode(buffer);
        record.setValue(idx, result.toString());
      } catch (Error e) {
        throw new StepException(
          String.format("Problem converting to character set '%s'", charset)
        );
      }
    }

    return records;
  }
}
