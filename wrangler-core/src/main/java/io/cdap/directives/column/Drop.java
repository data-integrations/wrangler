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

package io.cdap.directives.column;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.StageContext;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnNameList;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * This class <code>Drop</code> implements a directive that will drop
 * the list of columns specified.
 */
@Plugin(type = Directive.TYPE)
@Name(Drop.NAME)
@Categories(categories = { "column"})
@Description("Drop one or more columns.")
public class Drop implements Directive {
  public static final String NAME = "drop";

  // Columns to be dropped.
  private List<String> columns;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME_LIST);
    return builder.build();
  }

  @Override
  public List<FieldTransformOperation> getFieldOperations(StageContext context) {
    return Collections.singletonList(
      new FieldTransformOperation(String.format("Drop columns %s", columns),
                                  String.format("Drop the columns: %s", columns), columns));
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    ColumnNameList cols = args.value("column");
    columns = cols.value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {
    for (Row row : rows) {
      for (String column : columns) {
        int idx = row.find(column.trim());
        if (idx != -1) {
          row.remove(idx);
        }
      }
    }
    return rows;
  }
}

