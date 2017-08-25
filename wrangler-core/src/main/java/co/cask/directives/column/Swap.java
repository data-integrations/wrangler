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

package co.cask.directives.column;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.i18n.Messages;
import co.cask.wrangler.i18n.MessagesFactory;

import java.util.List;

/**
 * A directive for swapping the column names.
 */
@Plugin(type = Directive.Type)
@Name(Swap.NAME)
@Categories(categories = { "column"})
@Description("Swaps the column names of two columns.")
public class Swap implements Directive {
  public static final String NAME = "swap";
  private static final Messages MSG = MessagesFactory.getMessages();
  private String left;
  private String right;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("left", TokenType.COLUMN_NAME);
    builder.define("right", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    left = ((ColumnName) args.value("left")).value();
    right = ((ColumnName) args.value("right")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int sidx = row.find(left);
      int didx = row.find(right);

      if (sidx == -1) {
        throw new DirectiveExecutionException(MSG.get("column.not.found", toString(), left));
      }

      if (didx == -1) {
        throw new DirectiveExecutionException(MSG.get("column.not.found", toString(), right));
      }

      row.setColumn(sidx, right);
      row.setColumn(didx, left);
    }
    return rows;
  }
}
