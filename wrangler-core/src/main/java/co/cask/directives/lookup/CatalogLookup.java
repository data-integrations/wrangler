/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.lookup;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import co.cask.wrangler.executor.ICDCatalog;

import java.util.List;

/**
 * A directive that looks up ICD Code from the catalog.
 */
@Plugin(type = Directive.Type)
@Name(CatalogLookup.NAME)
@Description("Looks-up values from pre-loaded (static) catalogs.")
public class CatalogLookup implements Directive {
  public static final String NAME = "catalog-lookup";
  // StaticCatalog that holds the ICD code and their descriptions
  private StaticCatalog catalog;

  // Catalog name -- normalized for column name
  private String name;

  // Column from which the ICD code needs to be read.
  private String column;

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
      throw new DirectiveParseException("Invalid ICD type - should be 9 (ICD-9) or 10 (ICD-10-2016 " +
                                           "or ICD-10-2017).");
    } else {
      catalog = new ICDCatalog(type.toLowerCase());
      if (!catalog.configure()) {
        throw new DirectiveParseException(
          String.format("Failed to configure ICD StaticCatalog. Check with your administrator")
        );
      }
    }
    this.name = catalog.getCatalog().replaceAll("-", "_");
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
