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
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A directive for renaming columns.
 */
@Plugin(type = Directive.Type)
@Name(Rename.NAME)
@Description("Renames a column 'source' to 'target'")
public final class Rename implements Directive {
  public static final String NAME = "rename";
  private ColumnName source;
  private ColumnName target;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("target", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    source = args.value("source");
    if (args.contains("target")) {
      target = args.value("target");
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException, ErrorRowException {
    for (Row row : rows) {
      int idx = row.find(source.value());
      int idxnew = row.find(target.value());
      if (idx != -1) {
        if (idxnew == -1) {
          row.setColumn(idx, target.value());
        } else {
          throw new DirectiveExecutionException(
            String.format(
              "%s : %s column already exists. Apply the directive 'drop %s' before renaming %s to %s.", toString(),
              target.value(), target.value(), source.value(), source.value()
            )
          );
        }
      }
    }
    return rows;
  }
}
