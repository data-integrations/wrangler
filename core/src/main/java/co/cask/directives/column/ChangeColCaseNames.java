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
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.Identifier;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * This class <code>ChangeColCaseNames</code> converts the case of the columns
 * to either lower-case or uppercase.
 */
@Plugin(type = UDD.Type)
@Name(ChangeColCaseNames.NAME)
@Description("Changes the case of column names to either lowercase or uppercase.")
public class ChangeColCaseNames implements UDD {
  public static final String NAME = "change-column-case";
  private boolean toLower;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("case", TokenType.IDENTIFIER, Optional.TRUE);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    toLower = true;
    if (args.contains("case")) {
      Identifier identifier = args.value("case");
      String casing = identifier.value();
      if (casing.equalsIgnoreCase("upper") || casing.equalsIgnoreCase("uppercase")) {
        toLower = false;
      }
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      for (int i = 0; i < row.length(); ++i) {
        String name = row.getColumn(i);
        if (toLower) {
          row.setColumn(i, name.toLowerCase());
        } else {
          row.setColumn(i, name.toUpperCase());
        }
      }
    }
    return rows;
  }
}

