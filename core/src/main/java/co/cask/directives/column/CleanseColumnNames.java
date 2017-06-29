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
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.UDD;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A directive for cleanses columns names.
 *
 * <p>
 *   <ul>
 *     <li>Lowercases the column name</li>
 *     <li>Trims space</li>
 *     <li>Replace characters other than [A-Z][a-z][_] with empty string.</li>
 *   </ul>
 * </p>
 */
@Plugin(type = UDD.Type)
@Name(CleanseColumnNames.NAME)
@Description("Sanatizes column names: trims, lowercases, and replaces all but [A-Z][a-z][0-9]_." +
  "with an underscore '_'.")
public final class CleanseColumnNames implements UDD {
  public static final String NAME = "cleanse-column-names";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    // No-op.
  }

  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      for (int i = 0; i < row.length(); ++i) {
        String column = row.getColumn(i);
        // Trims
        column = column.trim();
        // Lower case columns
        column = column.toLowerCase();
        // Filtering unwanted characters
        column = column.replaceAll("[^a-zA-Z0-9_]", "_");
        row.setColumn(i, column);
      }
    }
    return rows;
  }
}
