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
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
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
public class SplitURL implements Directive, Lineage {
  public static final String NAME = "split-url";
  private String column;
  private String protocolCol;
  private String authCol;
  private String hostCol;
  private String portCol;
  private String pathCol;
  private String queryCol;
  private String fileCol;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.protocolCol = column + "_protocol";
    this.authCol = column + "_authority";
    this.hostCol = column + "_host";
    this.portCol = column + "_port";
    this.pathCol = column + "_path";
    this.queryCol = column + "_query";
    this.fileCol = column + "_filename";
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
          row.add(protocolCol, null);
          row.add(authCol, null);
          row.add(hostCol, null);
          row.add(portCol, null);
          row.add(pathCol, null);
          row.add(queryCol, null);
          row.add(fileCol, null);
          continue;
        }
        if (object instanceof String) {
          try {
            URL url = new URL((String) object);
            row.add(protocolCol, url.getProtocol());
            row.add(authCol, url.getAuthority());
            row.add(hostCol, url.getHost());
            row.add(portCol, url.getPort());
            row.add(pathCol, url.getPath());
            row.add(fileCol, url.getFile());
            row.add(queryCol, url.getQuery());
          } catch (MalformedURLException e) {
            throw new DirectiveExecutionException(
              NAME, String.format("Malformed url '%s' found in column '%s'.", (String) object, column), e);
          }
        } else {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has invalid type '%s'. It should be of type 'String'.",
                                column, object.getClass().getSimpleName()));
        }
      } else {
        throw new DirectiveExecutionException(NAME, String.format("Column '%s' does not exist.", column));
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable("Split column '%s' into url parts as columns '%s', '%s', '%s', '%s', '%s', '%s', '%s'",
                column, protocolCol, authCol, hostCol, portCol, pathCol, queryCol, fileCol)
      .relation(column, Many.of(column, protocolCol, authCol, hostCol, portCol, pathCol, queryCol, fileCol))
      .build();
  }
}
