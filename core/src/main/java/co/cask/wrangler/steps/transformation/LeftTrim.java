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

/**
 * A Wrangler step for trimming whitespace from left side of a string
 */
@Plugin(type = "udd")
@Name("ltrim")
@Usage("ltrim <column>")
@Description("Trimming whitespace from left side of a string.")
public class LeftTrim extends AbstractDirective {
  // Columns of the column to be upper-cased
  private String col;

  public LeftTrim(int lineno, String detail, String col) {
    super(lineno, detail);
    this.col = col;
  }

  /**
   * Trimming white spaces from left side of a column value
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Row} in which the 'col' value after trimming
   * @throws DirectiveExecutionException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object instanceof String) {
          if (object != null) {
            String value = (String) object;
            row.setValue(idx, Trimmer.ltrim(value));
          }
        }
      }
    }
    return rows;
  }
}
