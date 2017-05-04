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

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import co.cask.wrangler.steps.nlp.internal.PorterStemmer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Step for stemming the words provided using Porter Stemming.
 */
@Usage(
  directive = "stemming",
  usage = "stemming <column>",
  description = "Apply Porter Stemming on the column value."
)
public class Stemming extends AbstractStep {
  private final String column;
  private final PorterStemmer stemmer;

  public Stemming(int lineno, String detail, String column) {
    super(lineno, detail);
    this.column = column;
    this.stemmer = new PorterStemmer();
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records List of input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled List of {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      List<String> stemmed = new ArrayList<>();
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);
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
            record.add(String.format("%s_porter", column), stemmed);
          } catch (IOException e) {
            throw new StepException(
              String.format("%s : Unable to apply porter stemmer on column '%s'. %s", toString(), column,
                            e.getMessage())
            );
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String, String[] or List<String>.",
                          toString(), object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        record.add(String.format("%s_porter", column), stemmed);
      }
    }
    return records;
  }
}
