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

package co.cask.wrangler.steps.writer;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.wrangler.api.AbstractUnboundedInputStep;
import co.cask.wrangler.api.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A step to write the record fields as JSON.
 */
@Usage(
  directive = "write-as-json-map",
  usage = "write-as-json-map <column>",
  description = "Writes all record columns as JSON map."
)
public class WriteAsJsonMap extends AbstractUnboundedInputStep {
  private final String column;
  private final Gson gson;

  public WriteAsJsonMap(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
    this.gson = new Gson();
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
      Map<String, Object> toJson = new HashMap<>();
      for (KeyValue<String, Object> entry : record.getFields()) {
        toJson.put(entry.getKey(), entry.getValue());
      }
      record.addOrSet(column, gson.toJson(toJson));
    }
    return records;
  }

  @Override
  public Set<String> getBoundedOutputColumns() {
    return ImmutableSet.of(column);
  }

  @Override
  public Set<String> getOutputColumn(String inputColumn) {
    return ImmutableSet.of(column);
  }
}
