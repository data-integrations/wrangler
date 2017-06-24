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

package co.cask.wrangler.steps.transformation;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * A Directive to split a URL into it's components.
 */
@Plugin(type = "udd")
@Name("split-url")
@Usage("split-url <column>")
@Description("Split a url into it's components host,protocol,port,etc.")
public class SplitURL extends AbstractStep {
  private final String column;

  public SplitURL(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
  }

  /**
   * Executes a wrangle step on single {@link Record} and return an array of wrangled {@link Record}.
   *
   * @param records  Input {@link Record} to be wrangled by this step.
   * @param context {@link PipelineContext} passed to each step.
   * @return Wrangled {@link Record}.
   */
  @Override
  public List<Record> execute(List<Record> records, PipelineContext context) throws StepException {
    for (Record record : records) {
      int idx = record.find(column);
      if (idx != -1) {
        Object object = record.getValue(idx);

        if (object == null) {
          record.add(column + "_protocol", null);
          record.add(column + "_authority", null);
          record.add(column + "_host", null);
          record.add(column + "_port", null);
          record.add(column + "_path", null);
          record.add(column + "_query", null);
          record.add(column + "_filename", null);
          continue;
        }
        if (object instanceof String) {
          try {
            URL url = new URL((String) object);
            record.add(column + "_protocol", url.getProtocol());
            record.add(column + "_authority", url.getAuthority());
            record.add(column + "_host", url.getHost());
            record.add(column + "_port", url.getPort());
            record.add(column + "_path", url.getPath());
            record.add(column + "_filename", url.getFile());
            record.add(column + "_query", url.getQuery());
          } catch (MalformedURLException e) {
            throw new StepException(
              String.format(
                "Malformed url '%s' found in column '%s'", (String) object, column
              )
            );
          }
        } else {
          throw new StepException(
            String.format("%s : Invalid type '%s' of column '%s'. Should be of type String.", toString(),
                          object != null ? object.getClass().getName() : "null", column)
          );
        }
      } else {
        throw new StepException(toString() + " : Column '" + column + "' does not exist in the record.");
      }
    }
    return records;
  }
}
