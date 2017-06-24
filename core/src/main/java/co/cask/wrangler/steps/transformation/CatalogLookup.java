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
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * Looks ICD Code from the catalog.
 */
@Plugin(type = "udd")
@Name("catalog-lookup")
@Usage("catalog-lookup <catalog> <column>")
@Description("Looks-up values from pre-loaded (static) catalogs.")
public class CatalogLookup extends AbstractStep {
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
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws DirectiveExecutionException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
        if (object != null && object instanceof String) {
          String code = (String) object;
          StaticCatalog.Entry value = catalog.lookup(code);
          if (value != null) {
            record.add(String.format("%s_%s_description", column, name), value.getDescription());
          } else {
            record.add(String.format("%s_%s_description", column, name), null);
          }
        } else {
          record.add(String.format("%s_%s_description", column, name), null);
        }
      } else {
        record.add(String.format("%s_%s_description", column, name), null);
      }
    }
    return records;
  }
}
