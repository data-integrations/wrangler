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
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Optional;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.i18n.Messages;
import co.cask.wrangler.i18n.MessagesFactory;

import java.util.List;

/**
 * A directive for copying value of one column to another.
 */
@Plugin(type = Directive.Type)
@Name(Copy.NAME)
@Description("Copies values from a source column into a destination column.")
public class Copy implements Directive {
  private static final Messages MSG = MessagesFactory.getMessages();
  public static final String NAME = "copy";
  private ColumnName source;
  private ColumnName destination;
  private boolean force = false;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("force", TokenType.BOOLEAN, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.source = args.value("source");
    this.destination = args.value("destination");
    if (args.contains("force")) {
      force = (boolean) args.value("force").value();
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int sidx = row.find(source.value());
      if (sidx == -1) {
        throw new DirectiveExecutionException(MSG.get("column.not.found", toString(), source.value()));
      }

      int didx = row.find(destination.value());
      // If source and destination are same, then it's a nop.
      if (didx == sidx) {
        continue;
      }

      if (didx == -1) {
        // if destination column doesn't exist then add it.
        row.add(destination.value(), row.getValue(sidx));
      } else {
        // if destination column exists, and force is set to false, then throw exception, else
        // overwrite it.
        if (!force) {
          throw new DirectiveExecutionException(toString() + " : Destination column '" + destination.value()
                                    + "' does not exist in the row. Use 'force' option to add new column.");
        }
        row.setValue(didx, row.getValue(sidx));
      }
    }
    return rows;
  }
}
