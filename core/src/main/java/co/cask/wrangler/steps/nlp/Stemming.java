/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.steps.nlp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.annotations.Usage;
import co.cask.wrangler.steps.nlp.internal.PorterStemmer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Directive for stemming the words provided using Porter Stemming.
 */
@Plugin(type = "directives")
@Name("stemming")
@Usage("stemming <column>")
@Description("Apply Porter Stemming on the column value.")
public class Stemming extends AbstractDirective {
  private final String column;
  private final PorterStemmer stemmer;

  public Stemming(int lineno, String detail, String column) {
    super(lineno, detail);
    this.column = column;
    this.stemmer = new PorterStemmer();
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows List of input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled List of {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
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
