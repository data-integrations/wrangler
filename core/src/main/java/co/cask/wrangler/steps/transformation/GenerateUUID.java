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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.Usage;

import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * A step to generate a UUID.
 */
@Plugin(type = "udd")
@Name("generate-uuid")
@Usage("generate-uuid <column>")
@Description("Populates a column with a universally unique identifier (UUID) of the record.")
public class GenerateUUID extends AbstractDirective {
  private final String column;
  private final Random random;

  public GenerateUUID(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
    this.random = new Random();
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows  Input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      UUID uuid = new UUID(random.nextLong(), random.nextLong());
      if (idx != -1) {
        row.setValue(idx, uuid.toString());
      } else {
        row.add(column, uuid.toString());
      }
    }
    return rows;
  }
}
