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
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;

import java.util.List;

/**
 * Looks ICD Code from the catalog.
 */
@Plugin(type = "udd")
@Name("catalog-lookup")
@Usage("catalog-lookup <catalog> <column>")
@Description("Looks-up values from pre-loaded (static) catalogs.")
public class CatalogLookup extends AbstractDirective {
  // StaticCatalog that holds the ICD code and their descriptions
  private StaticCatalog catalog;

  // Catalog name -- normalized for column name
  private final String name;

  // Column from which the ICD code needs to be read.
  private final String column;

  public CatalogLookup(int lineno, String detail, StaticCatalog catalog, String column) {
    super(lineno, detail);
    this.column = column;
    this.catalog = catalog;
    this.name = catalog.getCatalog().replaceAll("-", "_");
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object != null && object instanceof String) {
          String code = (String) object;
          StaticCatalog.Entry value = catalog.lookup(code);
          if (value != null) {
            row.add(String.format("%s_%s_description", column, name), value.getDescription());
          } else {
            row.add(String.format("%s_%s_description", column, name), null);
          }
        } else {
          row.add(String.format("%s_%s_description", column, name), null);
        }
      } else {
        row.add(String.format("%s_%s_description", column, name), null);
      }
    }
    return rows;
  }
}
