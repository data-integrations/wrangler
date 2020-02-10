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
import io.cdap.wrangler.api.Pair;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Many;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * A directive to split email address into account and domain.
 */
@Plugin(type = Directive.TYPE)
@Name(SplitEmail.NAME)
@Categories(categories = { "transform", "email"})
@Description("Split a email into account and domain.")
public class SplitEmail implements Directive, Lineage {
  public static final String NAME = "split-email";
  private String column;
  private String generatedAccountCol;
  private String generatedDomainCol;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.generatedAccountCol = column + "_account";
    this.generatedDomainCol = column + "_domain";
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
          row.add(generatedAccountCol, null);
          row.add(generatedDomainCol, null);
          continue;
        }
        if (object instanceof String) {
          String emailAddress = (String) object;
          int nameIdx = emailAddress.lastIndexOf("<"); // Joltie, Root <joltie.root@yahoo.com>
          if (nameIdx == -1) {
            Pair<String, String> components = extractDomainAndAccount(emailAddress);
            row.add(generatedAccountCol, components.getFirst());
            row.add(generatedDomainCol, components.getSecond());
          } else {
            String name = emailAddress.substring(0, nameIdx);
            int endIdx = emailAddress.lastIndexOf(">");
            if (endIdx == -1) {
              row.add(generatedAccountCol, null);
              row.add(generatedDomainCol, null);
            } else {
              emailAddress = emailAddress.substring(nameIdx + 1, endIdx);
              Pair<String, String> components = extractDomainAndAccount(emailAddress);
              row.add(generatedAccountCol, components.getFirst());
              row.add(generatedDomainCol, components.getSecond());
            }
          }
        } else {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has invalid type '%s'. " +
                                  "It should be of type 'String'.", column, object.getClass().getSimpleName()));
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
      .readable("Split column '%s' into columns '%s' and '%s'",
                column, generatedAccountCol, generatedDomainCol)
      .relation(column, Many.of(column, generatedAccountCol, generatedDomainCol))
      .build();
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
