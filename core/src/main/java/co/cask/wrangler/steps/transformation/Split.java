/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Usage;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangler step for splitting a col into two additional columns based on a delimiter.
 */
@Plugin(type = "udd")
@Name("split")
@Usage("split <source> <delimiter> <new-column-1> <new-column-2>")
@Description("[DEPRECATED] Use 'split-to-columns' or 'split-to-rows'.")
public class Split extends AbstractDirective {
  // Name of the column to be split
  private String col;

  private String delimiter;

  // Destination column names
  private String firstColumnName, secondColumnName;

  public Split(int lineno, String detail, String col,
               String delimiter, String firstColumnName, String secondColumnName) {
    super(lineno, detail);
    this.col = col;
    this.delimiter = delimiter;
    this.firstColumnName = firstColumnName;
    this.secondColumnName = secondColumnName;
  }

  /**
   * Splits column based on the delimiter into two columns.
   *
   * @param rows Input {@link Row} to be wrangled by this step.
   * @param context Specifies the context of the pipeline.
   * @return Transformed {@link Row} which contains two additional columns based on the split
   * @throws DirectiveExecutionException thrown when type of 'col' is not STRING.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      int idx = row.find(col);
      if (idx != -1) {
        String val = (String) row.getValue(idx);
        if (val != null) {
          String[] parts = val.split(delimiter, 2);
          if (Strings.isNullOrEmpty(parts[0])) {
            row.add(firstColumnName, parts[1]);
            row.add(secondColumnName, null);
          } else {
            row.add(firstColumnName, parts[0]);
            row.add(secondColumnName, parts[1]);
          }
        } else {
          row.add(firstColumnName, null);
          row.add(secondColumnName, null);
        }
      } else {
        throw new DirectiveExecutionException(
          col + " is not of type string. Please check the wrangle configuration."
        );
      }
      results.add(row);
    }
    return results;
  }
}
