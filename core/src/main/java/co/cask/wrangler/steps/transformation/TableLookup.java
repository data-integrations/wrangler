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
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.etl.api.Lookup;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Usage;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An AbstractDirective that performs a lookup into a Table Dataset and adds the row values into the record.
 */
@Plugin(type = "udd")
@Name("table-lookup")
@Usage("table-lookup <column> <table>")
@Description("Uses the given column as a key to perform a lookup into the specified table.")
public class TableLookup extends AbstractDirective {

  private final String column;
  private final String table;

  private boolean initialized;
  private co.cask.cdap.etl.api.lookup.TableLookup tableLookup;

  public TableLookup(int lineno, String directive, String column, String table) {
    super(lineno, directive);
    this.column = column;
    this.table = table;
    this.initialized = false;
  }

  private void ensureInitialized(RecipeContext context) throws DirectiveExecutionException {
    if (initialized) {
      return;
    }
    Lookup lookup;
    try {
      lookup = context.provide(table, Collections.<String, String>emptyMap());
    } catch (DatasetInstantiationException e) {
      throw new DirectiveExecutionException(
        String.format("%s : Please check that a dataset '%s' of type Table exists.",
        toString(), table));
    }
    if (!(lookup instanceof co.cask.cdap.etl.api.lookup.TableLookup)) {
      throw new DirectiveExecutionException(toString() + " : Lookup can be performed only on Tables.");
    }
    tableLookup = (co.cask.cdap.etl.api.lookup.TableLookup) lookup;
    initialized = true;
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
    ensureInitialized(context);
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        throw new DirectiveExecutionException(toString() + " : Column '" + column + "' does not exist in the row.");
      }
      Object object = row.getValue(idx);
      if (!(object instanceof String)) {
        throw new DirectiveExecutionException(
          String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                        object != null ? object.getClass().getName() : "null", column)
        );
      }
      co.cask.cdap.api.dataset.table.Row lookedUpRow = tableLookup.lookup((String) object);
      for (Map.Entry<byte[], byte[]> entry : lookedUpRow.getColumns().entrySet()) {
        row.add(column + "_" + Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
      }
    }
    return rows;
  }
}
