/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.Usage;

import java.util.Arrays;
import java.util.List;

/**
 * A step for dropping columns.
 *
 * This step will create a copy of the input {@link Record} and clears
 * all previous column names and add new column names.
 */

@Plugin(type = "udd")
@Name("drop")
@Usage("drop <column>[,<column>*]")
@Description("Drop one or more columns.")
public class Drop extends AbstractDirective {
  // Columns to be dropped.
  private List<String> columns;

  public Drop(int lineno, String detail, String column) {
    this(lineno, detail, Arrays.asList(column));
  }

  public Drop(int lineno, String detail, List<String> columns) {
    super(lineno, detail);
    this.columns = columns;
  }

  /**
   * Drops the columns specified.
   *
   * @param records Input {@link Record} to be wrangled by this step
   * @param context Specifies the context of the pipeline
   * @return A newly-transformed {@link Record}
   * @throws DirectiveExecutionException
   */
  @Override
  public List<Record> execute(List<Record> records, RecipeContext context)
    throws DirectiveExecutionException {
    for (Record record : records) {
      for (String column : columns) {
        int idx = record.find(column.trim());
        if (idx != -1) {
          record.remove(idx);
        }
      }
    }
    return records;
  }
}

