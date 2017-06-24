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
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;

import java.util.List;

/**
 * A Directive to split email address into account and domain.
 */
@Plugin(type = "udd")
@Name("split-email")
@Usage("split-email <column>")
@Description("Split a email into account and domain.")
public class SplitEmail extends AbstractStep {
  private final String column;

  public SplitEmail(int lineno, String directive, String column) {
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
          record.add(column + "_account", null);
          record.add(column + "_domain", null);
          continue;
        }
        if (object instanceof String) {
          String emailAddress = (String) object;
          int nameIdx = emailAddress.lastIndexOf("<"); // Joltie, Root <joltie.root@yahoo.com>
          if (nameIdx == -1) {
            Pair<String, String> components = extractDomainAndAccount(emailAddress);
            record.add(column + "_account", components.getFirst());
            record.add(column + "_domain", components.getSecond());
          } else {
            String name = emailAddress.substring(0, nameIdx);
            int endIdx = emailAddress.lastIndexOf(">");
            if (endIdx == -1) {
              record.add(column + "_account", null);
              record.add(column + "_domain", null);
            } else {
              emailAddress = emailAddress.substring(nameIdx + 1, endIdx);
              Pair<String, String> components = extractDomainAndAccount(emailAddress);
              record.add(column + "_account", components.getFirst());
              record.add(column + "_domain", components.getSecond());
            }
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

  private Pair<String, String> extractDomainAndAccount(String emailId) {
    int lastidx = emailId.lastIndexOf("@");
    if (lastidx == -1) {
      return new Pair<>(null, null);
    } else {
      return new Pair<>(emailId.substring(0, lastidx), emailId.substring(lastidx + 1));
    }
  }
}
