/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.language;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

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
@Plugin(type = Directive.Type)
@Name("set-charset")
@Categories(categories = {"language"})
@Description("Sets the character set decoding to UTF-8.")
public class SetCharset implements Directive {
  public static final String NAME = "set-charset";
  private String column;
  private String charset;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("charset", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.charset = ((Text) args.value("charset")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException,
    ErrorRowException {

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
