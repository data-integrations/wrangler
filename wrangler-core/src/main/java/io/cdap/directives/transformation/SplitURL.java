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

package io.cdap.directives.transformation;

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
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * A directive to split a URL into it's components.
 */
@Plugin(type = Directive.TYPE)
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
