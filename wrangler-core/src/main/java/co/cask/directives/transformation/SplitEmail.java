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
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Categories;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A directive to split email address into account and domain.
 */
@Plugin(type = Directive.Type)
@Name(SplitEmail.NAME)
@Categories(categories = { "transform", "email"})
@Description("Split a email into account and domain.")
public class SplitEmail implements Directive {
  public static final String NAME = "split-email";
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
          row.add(column + "_account", null);
          row.add(column + "_domain", null);
          continue;
        }
        if (object instanceof String) {
          String emailAddress = (String) object;
          int nameIdx = emailAddress.lastIndexOf("<"); // Joltie, Root <joltie.root@yahoo.com>
          if (nameIdx == -1) {
            Pair<String, String> components = extractDomainAndAccount(emailAddress);
            row.add(column + "_account", components.getFirst());
            row.add(column + "_domain", components.getSecond());
          } else {
            String name = emailAddress.substring(0, nameIdx);
            int endIdx = emailAddress.lastIndexOf(">");
            if (endIdx == -1) {
              row.add(column + "_account", null);
              row.add(column + "_domain", null);
            } else {
              emailAddress = emailAddress.substring(nameIdx + 1, endIdx);
              Pair<String, String> components = extractDomainAndAccount(emailAddress);
              row.add(column + "_account", components.getFirst());
              row.add(column + "_domain", components.getSecond());
            }
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

  private Pair<String, String> extractDomainAndAccount(String emailId) {
    int lastidx = emailId.lastIndexOf("@");
    if (lastidx == -1) {
      return new Pair<>(null, null);
    } else {
      return new Pair<>(emailId.substring(0, lastidx), emailId.substring(lastidx + 1));
    }
  }
}
