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

package co.cask.directives.nlp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.directives.nlp.internal.PorterStemmer;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Executor for stemming the words provided using Porter Stemming.
 */
@Plugin(type = Directive.Type)
@Name("stemming")
@Categories(categories = { "nlp"})
@Description("Apply Porter Stemming on the column value.")
public class Stemming implements Directive {
  public static final String NAME = "stemming";
  private String column;
  private PorterStemmer stemmer;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.column = ((ColumnName) args.value("column")).value();
    this.stemmer = new PorterStemmer();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      List<String> stemmed = new ArrayList<>();
      int idx = row.find(column);
      if (idx != -1) {
        Object object = row.getValue(idx);
        if (object != null && (object instanceof List || object instanceof String[] || object instanceof String)) {
          List<String> words = null;
          if (object instanceof String[]) {
            words = Arrays.asList((String[]) object);
          } else if (object instanceof List) {
            words = (List<String>) object;
          } else {
            String phrase = (String) object;
            String[] w = phrase.split("\\W+");
            words = Arrays.asList(w);
          }
          try {
            stemmed = stemmer.process(words);
            row.add(String.format("%s_porter", column), stemmed);
          } catch (IOException e) {
            throw new DirectiveExecutionException(
              String.format("%s : Unable to apply porter stemmer on column '%s'. %s", toString(), column,
                            e.getMessage())
            );
          }
        } else {
          throw new DirectiveExecutionException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String, String[] or List<String>.",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        row.add(String.format("%s_porter", column), stemmed);
      }
    }
    return rows;
  }
}
