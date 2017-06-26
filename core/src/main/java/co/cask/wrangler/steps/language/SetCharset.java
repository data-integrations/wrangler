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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.ErrorRecordException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;

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
@Plugin(type = "udd")
@Name("set-charset")
@Usage("set-charset <column> <charset>")
@Description("Sets the character set decoding to UTF-8.")
public class SetCharset extends AbstractDirective {
  private final String column;
  private final String charset;

  public SetCharset(int lineno, String detail, String column, String charset) {
    super(lineno, detail);
    this.column = column;
    this.charset = charset;
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException,
    ErrorRecordException {

    // Iterate through all the rows.
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue;
      }

      Object object = row.getValue(idx);
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
        throw new DirectiveExecutionException(
          String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                        object != null ? object.getClass().getName() : "null", column)

        );
      }

      try {
        CharBuffer result = Charset.forName(charset).decode(buffer);
        row.setValue(idx, result.toString());
      } catch (Error e) {
        throw new DirectiveExecutionException(
          String.format("Problem converting to character set '%s'", charset)
        );
      }
    }

    return rows;
  }
}
