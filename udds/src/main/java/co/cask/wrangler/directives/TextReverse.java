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

package co.cask.wrangler.directives;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * This class <code>TextReverse</code>implements a directive for reversing the
 * text specified by the value of the <code>column</code>.
 */
@Plugin(type = UDD.Type)
@Name(TextReverse.DIRECTIVE_NAME)
@Description("Reverses the text represented by the column.")
public final class TextReverse implements UDD {
  public static final String DIRECTIVE_NAME = "text-reverse";
  private ColumnName column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(DIRECTIVE_NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args)
    throws DirectiveParseException {
    if (!args.contains("column")) {
      throw new DirectiveParseException(
        String.format(
          "%d:%d - '%s' is required columns for the directive '%s'", args.line(),
          args.column(), "column", DIRECTIVE_NAME
        )
      );
    }
    column = args.value("column");
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context)
    throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(column.value());
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          if (object != null) {
            String value = (String) object;
            String reversed = new StringBuffer(value).reverse().toString();
            row.setValue(idx, reversed);
          }
        } else if (object instanceof byte[]) {
          String value = Bytes.toString((byte[])object);
          String reversed = new StringBuffer(value).reverse().toString();
          row.setValue(idx, reversed);
        }
      }
    }
    return rows;
  }
}
