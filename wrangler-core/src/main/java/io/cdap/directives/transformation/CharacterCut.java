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
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import org.unix4j.Unix4j;

import java.util.List;

/**
 * A directive implements unix cut directive.
 */
@Plugin(type = Directive.TYPE)
@Name(CharacterCut.NAME)
@Categories(categories = { "transform"})
@Description("UNIX-like 'cut' directive for splitting text.")
public class CharacterCut implements Directive, Lineage {
  public static final String NAME = "cut-character";
  private String source;
  private String destination;
  private String range;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("source", TokenType.COLUMN_NAME);
    builder.define("destination", TokenType.COLUMN_NAME);
    builder.define("ranges", TokenType.TEXT);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.source = ((ColumnName) args.value("source")).value();
    this.destination = ((ColumnName) args.value("destination")).value();
    this.range = ((Text) args.value("ranges")).value();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      int idx = row.find(source);
      if (idx != -1) {
        Object value = row.getValue(idx);

        if (value == null) {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has null value. It should be a non-null 'String'.", source));
        }

        if (value instanceof String) {
          String result = Unix4j.fromString((String) value).cut("-c", range).toStringResult();
          row.addOrSet(destination, result);
        } else {
          throw new DirectiveExecutionException(
            NAME, String.format("Column '%s' has invalid type '%s'. It should be of type 'String'.",
                                source, value.getClass().getSimpleName()));
        }
      } else {
        throw new DirectiveExecutionException(NAME, "Scope column '" + source + "' does not exist.");
      }
    }
    return rows;
  }

  @Override
  public Mutation lineage() {
    return Mutation.builder()
      .readable(String.format("Character cut from column %s to destination %s using range %s",
                              source, destination, range))
      .conditional(source, destination)
      .build();
  }
}
