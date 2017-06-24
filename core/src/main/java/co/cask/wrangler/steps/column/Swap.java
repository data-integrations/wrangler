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
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.i18n.Messages;
import co.cask.wrangler.i18n.MessagesFactory;

import java.util.List;

/**
 * A step for swapping the column names.
 */
@Plugin(type = "udd")
@Name("swap")
@Usage("swap <column1> <column2>")
@Description("Swaps the column names of two columns.")
public class Swap extends AbstractDirective {
  private static final Messages MSG = MessagesFactory.getMessages();
  private final String column1;
  private final String column2;

  public Swap(int lineno, String directive, String column1, String column2) {
    super(lineno, directive);
    this.column1 = column1;
    this.column2 = column2;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, RecipeContext context) throws DirectiveExecutionException {
    for (Record record : records) {
      int sidx = record.find(column1);
      int didx = record.find(column2);

      if (sidx == -1) {
        throw new DirectiveExecutionException(MSG.get("column.not.found", toString(), column1));
      }

      if (didx == -1) {
        throw new DirectiveExecutionException(MSG.get("column.not.found", toString(), column2));
      }

      record.setColumn(sidx, column2);
      record.setColumn(didx, column1);
    }
    return records;
  }
}
