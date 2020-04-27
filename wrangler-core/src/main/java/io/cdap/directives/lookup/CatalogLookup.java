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
import io.cdap.wrangler.executor.ICDCatalog;

import java.util.List;

/**
 * A directive that looks up ICD Code from the catalog.
 */
@Plugin(type = Directive.TYPE)
@Name(CatalogLookup.NAME)
@Categories(categories = { "lookup"})
@Description("Looks-up values from pre-loaded (static) catalogs.")
public class CatalogLookup implements Directive, Lineage {
  public static final String NAME = "catalog-lookup";
  // StaticCatalog that holds the ICD code and their descriptions
  private StaticCatalog catalog;

  // Catalog name -- normalized for column name
  private String name;

  // Column from which the ICD code needs to be read.
  private String column;

  // This is the generated column
  private String generatedColumn;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("catalog", TokenType.TEXT);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    String type = ((Text) args.value("catalog")).value();
    if (!type.equalsIgnoreCase("ICD-9") && !type.equalsIgnoreCase("ICD-10-2016") &&
      !type.equalsIgnoreCase("ICD-10-2017")) {
      throw new DirectiveParseException(
        NAME, "Invalid ICD type - should be 9 (ICD-9) or 10 (ICD-10-2016 or ICD-10-2017).");
    } else {
      catalog = new ICDCatalog(type.toLowerCase());
      if (!catalog.configure()) {
        throw new DirectiveParseException(NAME, "Failed to configure ICD StaticCatalog. Check with your administrator");
      }
    }
    this.name = catalog.getCatalog().replaceAll("-", "_");
    this.generatedColumn = String.format("%s_%s_description", column, name);
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object != null && object instanceof String) {
          String code = (String) object;
          StaticCatalog.Entry value = catalog.lookup(code);
          if (value != null) {
            row.add(generatedColumn, value.getDescription());
          } else {
            row.add(generatedColumn, null);
          }
        } else {
          row.add(generatedColumn, null);
        }
      } else {
        row.add(generatedColumn, null);
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Looked up catalog using value in column '%s' and wrote it to column '%s'", column, generatedColumn)
      .relation(column, Many.of(column, generatedColumn))
      .build();
  }
}
