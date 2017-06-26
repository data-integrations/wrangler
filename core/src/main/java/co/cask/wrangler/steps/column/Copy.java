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

package co.cask.wrangler.steps.column;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.i18n.Messages;
import co.cask.wrangler.i18n.MessagesFactory;

import java.util.List;

/**
 * A step to copy data from one column to another.
 */
@Plugin(type = "udd")
@Name("copy")
@Usage("copy <source> <destination> [<force=true|false>]")
@Description("Copies values from a source column into a destination column.")
public class Copy extends AbstractDirective {
  private static final Messages MSG = MessagesFactory.getMessages();
  private String source;
  private String destination;
  private boolean force;

  public Copy(int lineno, String detail, String source, String destination, boolean force) {
    super(lineno, detail);
    this.source = source;
    this.destination = destination;
    this.force = force;
  }

  /**
   * Copies data from one column to another.
   *
   * If the destination column doesn't exist then it will create one, else it will
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Row} in which the 'col' value is lower cased.
   * @throws DirectiveExecutionException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int sidx = row.find(source);
      if (sidx == -1) {
        throw new DirectiveExecutionException(MSG.get("column.not.found", toString(), source));
      }

      int didx = row.find(destination);
      // If source and destination are same, then it's a nop.
      if (didx == sidx) {
        continue;
      }

      if (didx == -1) {
        // if destination column doesn't exist then add it.
        row.add(destination, row.getValue(sidx));
      } else {
        // if destination column exists, and force is set to false, then throw exception, else
        // overwrite it.
        if (!force) {
          throw new DirectiveExecutionException(toString() + " : Destination column '" + destination
                                    + "' does not exist in the row. Use 'force' option to add new column.");
        }
        row.setValue(didx, row.getValue(sidx));
      }

    }
    return rows;
  }
}
