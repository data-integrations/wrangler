/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.lookup;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.DatasetInstantiationException;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An directive that performs a lookup into a Table Dataset and adds the row values into the record.
 */
@Plugin(type = Directive.TYPE)
@Name(TableLookup.NAME)
@Categories(categories = { "lookup"})
@Description("Uses the given column as a key to perform a lookup into the specified table.")
public class TableLookup implements Directive, Lineage {
  public static final String NAME = "table-lookup";
  private String column;
  private String table;

  private boolean initialized;
  private io.cdap.cdap.etl.api.lookup.TableLookup tableLookup;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("table", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.table = ((Text) args.value("table")).value();
    this.initialized = false;
  }

  @Override
  public void destroy() {
    // no-op
  }

  private void ensureInitialized(ExecutorContext context) throws DirectiveExecutionException {
    if (initialized) {
      return;
    }
    Lookup lookup;
    try {
      lookup = context.provide(table, Collections.<String, String>emptyMap());
    } catch (DatasetInstantiationException e) {
      throw new DirectiveExecutionException(
        NAME, String.format("Dataset '%s' could not be instantiated. Make sure that a dataset '%s' of " +
                              "type Table exists.", table, table), e);
    }
    if (!(lookup instanceof io.cdap.cdap.etl.api.lookup.TableLookup)) {
      throw new DirectiveExecutionException(
        NAME, "Lookup is not being performed on a table. Lookup can be performed only on tables.");
    }
    tableLookup = (io.cdap.cdap.etl.api.lookup.TableLookup) lookup;
    initialized = true;
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    ensureInitialized(context);
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        continue;
      }
      Object object = row.getValue(idx);
      if (object == null) {
        throw new DirectiveExecutionException(
          NAME, String.format("Column '%s' has null value. It should be a non-null 'String'.", column)
        );
      }

      if (!(object instanceof String)) {
        throw new DirectiveExecutionException(
          NAME, String.format("Column '%s' is of invalid type '%s'. It should be of type 'String'.",
                              column, object.getClass().getSimpleName())
        );
      }
      io.cdap.cdap.api.dataset.table.Row lookedUpRow = tableLookup.lookup((String) object);
      for (Map.Entry<byte[], byte[]> entry : lookedUpRow.getColumns().entrySet()) {
        row.add(column + "_" + Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Looking up row in table '%s' based on column '%s'", table, column)
      .all(Many.of(column))
      .build();
  }
}
