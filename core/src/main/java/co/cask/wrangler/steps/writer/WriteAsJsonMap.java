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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.StepException;
import co.cask.wrangler.api.Usage;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A step to write the record fields as JSON.
 */
@Plugin(type = "udd")
@Name("write-as-json-map")
@Usage("write-as-json-map <column>")
@Description("Writes all record columns as JSON map.")
public class WriteAsJsonMap extends AbstractStep {
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
      for (Pair<String, Object> entry : record.getFields()) {
        toJson.put(entry.getFirst(), entry.getSecond());
      }
      record.addOrSet(column, gson.toJson(toJson));
    }
    return records;
  }
}
