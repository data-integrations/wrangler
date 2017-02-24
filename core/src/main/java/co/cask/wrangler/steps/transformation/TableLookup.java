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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.etl.api.Lookup;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An AbstractStep that performs a lookup into a Table Dataset and adds the row values into the record.
 */
@Usage(
  directive = "table-lookup",
  usage = "table-lookup <column> <table>",
  description = "Uses the given column as a key to perform a lookup into the specified table."
)
public class TableLookup extends AbstractStep {

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

  private void ensureInitialized(PipelineContext context) throws StepException {
    if (initialized) {
      return;
    }
    Lookup lookup;
    try {
      lookup = context.provide(table, Collections.<String, String>emptyMap());
    } catch (DatasetInstantiationException e) {
      throw new StepException(
        String.format("%s : Got exception while getting dataset '%s'. Please check that it exists.",
        toString(), table));
    }
    if (!(lookup instanceof co.cask.cdap.etl.api.lookup.TableLookup)) {
      throw new StepException(toString() + " : Only Table Lookup is supported.");
    }
    tableLookup = (co.cask.cdap.etl.api.lookup.TableLookup) lookup;
    initialized = true;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    ensureInitialized(context);
    for (Record record : records) {
      int idx = record.find(column);
      if (idx == -1) {
        throw new StepException(toString() + " : Column '" + column + "' does not exist in the record.");
      }
      Object object = record.getValue(idx);
      if (!(object instanceof String)) {
        throw new StepException(
          String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                        object != null ? object.getClass().getName() : "null", column)
        );
      }
      Row lookedUpRow = tableLookup.lookup((String) object);
      for (Map.Entry<byte[], byte[]> entry : lookedUpRow.getColumns().entrySet()) {
        record.add(column + "_" + Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
      }
    }
    return records;
  }
}
