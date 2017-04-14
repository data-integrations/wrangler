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

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.AbstractSimpleStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A Step to split email address into account and domain.
 */
@Usage(
  directive = "split-email",
  usage = "split-email <column>",
  description = "Split a email into account and domain."
)
public class SplitEmail extends AbstractSimpleStep {
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
            KeyValue<String, String> components = extractDomainAndAccount(emailAddress);
            record.add(column + "_account", components.getKey());
            record.add(column + "_domain", components.getValue());
          } else {
            int endIdx = emailAddress.lastIndexOf(">");
            if (endIdx == -1) {
              record.add(column + "_account", null);
              record.add(column + "_domain", null);
            } else {
              emailAddress = emailAddress.substring(nameIdx + 1, endIdx);
              KeyValue<String, String> components = extractDomainAndAccount(emailAddress);
              record.add(column + "_account", components.getKey());
              record.add(column + "_domain", components.getValue());
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

  private KeyValue<String, String> extractDomainAndAccount(String emailId) {
    int lastidx = emailId.lastIndexOf("@");
    if (lastidx == -1) {
      return new KeyValue<>(null, null);
    } else {
      return new KeyValue<>(emailId.substring(0, lastidx), emailId.substring(lastidx + 1));
    }
  }

  @Override
  public Map<String, Set<String>> getColumnMap() {
    Set<String> inputSet = ImmutableSet.of(column);
    return ImmutableMap.of(column + "_account", inputSet, column + "_domain", inputSet);
  }
}
