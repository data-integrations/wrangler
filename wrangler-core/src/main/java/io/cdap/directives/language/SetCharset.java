/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.directives.language;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

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
@Plugin(type = Directive.TYPE)
@Name("set-charset")
@Categories(categories = {"language"})
@Description("Sets the character set decoding to UTF-8.")
public class SetCharset implements Directive, Lineage {
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
          NAME, String.format("Column '%s' is of invalid type '%s'. It should be of type 'byte array' or " +
                                "'ByteBuffer'.", column, object.getClass().getSimpleName()));
      }

      try {
        CharBuffer result = Charset.forName(charset).decode(buffer);
        row.setValue(idx, result.toString());
      } catch (Error e) {
        throw new DirectiveExecutionException(
          NAME, String.format("Can not convert to character set '%s', %s", charset, e.getMessage()), e);
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Changed character set of column '%s' to '%s'", column, charset)
      .relation(column, column)
      .build();
  }
}
