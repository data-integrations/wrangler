/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.dataset.workspace;

import com.google.gson.Gson;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.FieldType;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.wrangler.api.DirectiveConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Stores the DirectiveConfig and other config settings.
 *
 * The actual store just has two columns -- key and value.
 * Currently the only thing it stores is the serialized DirectiveConfig in the row where key == 'directives'.
 * TODO: (CDAP-14619) check if the DirectiveConfig is used by anything/anyone. If so, see if it can be moved to app
 *   configuration instead of stored in a one row table.
 */
@Deprecated
public class ConfigStore {
  private static final Gson GSON = new Gson();
  private static final String KEY_COL = "key";
  private static final String VAL_COL = "value";
  private static final Field<String> keyField = Fields.stringField(KEY_COL, "directives");
  public static final StructuredTableId TABLE_ID = new StructuredTableId("dataprep_config");
  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(new FieldType(KEY_COL, FieldType.Type.STRING), new FieldType(VAL_COL, FieldType.Type.STRING))
    .withPrimaryKeys(KEY_COL)
    .build();
  private final StructuredTable table;

  public ConfigStore(StructuredTable table) {
    this.table = table;
  }

  public static ConfigStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new ConfigStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
  }

  public void updateConfig(DirectiveConfig config) throws IOException {
    List<Field<?>> fields = new ArrayList<>(2);
    fields.add(keyField);
    fields.add(Fields.stringField(VAL_COL, GSON.toJson(config)));
    table.upsert(fields);
  }

  public DirectiveConfig getConfig() throws IOException {
    Optional<StructuredRow> row = table.read(Collections.singletonList(keyField));
    String configStr = row.map(r -> r.getString(VAL_COL)).orElse("{}");
    return GSON.fromJson(configStr, DirectiveConfig.class);
  }
}
