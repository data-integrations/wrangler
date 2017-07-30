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

package co.cask.directives.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.Text;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import org.json.JSONObject;

import java.util.List;

/**
 * A directive to fill null or empty column values with a fixed value.
 */
@Plugin(type = Directive.Type)
@Name(FillNullOrEmpty.NAME)
@Categories(categories = { "transform"})
@Description("Fills a value of a column with a fixed value if it is either null or empty.")
public class FillNullOrEmpty implements Directive {
  public static final String NAME = "fill-null-or-empty";
  private String column;
  private String value;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    builder.define("value", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.value = ((Text) args.value("value")).value();
    if (value != null && value.isEmpty()) {
      throw new DirectiveParseException(
        "Fixed value cannot be a empty string"
      );
    }
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context)
    throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(column);
      if (idx == -1) {
        row.add(column, value);
        continue;
      }
      Object object = row.getValue(idx);
      if (object == null) {
        row.setValue(idx, value);
      } else {
        if (object instanceof String) {
          if (((String) object).isEmpty()) {
            row.setValue(idx, value);
          }
        } else if (object instanceof JSONObject) {
          if (JSONObject.NULL.equals(object)) {
            row.setValue(idx, value);
          }
        }
      }
    }
    return rows;
  }
}
