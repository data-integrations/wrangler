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
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * A directive to split a URL into it's components.
 */
@Plugin(type = Directive.Type)
@Name(SplitURL.NAME)
@Categories(categories = { "transform", "url"})
@Description("Split a url into it's components host,protocol,port,etc.")
public class SplitURL implements Directive {
  public static final String NAME = "split-url";
  private String column;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
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

        if (object == null) {
          row.add(column + "_protocol", null);
          row.add(column + "_authority", null);
          row.add(column + "_host", null);
          row.add(column + "_port", null);
          row.add(column + "_path", null);
          row.add(column + "_query", null);
          row.add(column + "_filename", null);
          continue;
        }
        if (object instanceof String) {
          try {
            URL url = new URL((String) object);
            row.add(column + "_protocol", url.getProtocol());
            row.add(column + "_authority", url.getAuthority());
            row.add(column + "_host", url.getHost());
            row.add(column + "_port", url.getPort());
            row.add(column + "_path", url.getPath());
            row.add(column + "_filename", url.getFile());
            row.add(column + "_query", url.getQuery());
          } catch (MalformedURLException e) {
            throw new DirectiveExecutionException(
              String.format(
                "Malformed url '%s' found in column '%s'", (String) object, column
              )
            );
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        throw new DirectiveExecutionException(toString() + " : Column '" + column + "' does not exist in the row.");
      }
    }
    return rows;
  }
}
