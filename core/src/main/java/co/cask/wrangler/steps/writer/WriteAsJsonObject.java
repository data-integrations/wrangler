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
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.pipeline.PipelineContext;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Usage;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.List;

/**
 * A directive for writing selected columns as Json Objects.
 */
@Plugin(type = "udd")
@Name("write-as-json-object")
@Usage("write-as-json-object <dest-column> [<src-column>[,<src-column>]")
@Description("Creates a JSON object based on source columns specified. JSON object is written into dest-column.")
public class WriteAsJsonObject extends AbstractStep {
  private final String column;
  private final List<String> columns;
  private final Gson gson;

  public WriteAsJsonObject(int lineno, String directive, String column, List<String> columns) {
    super(lineno, directive);
    this.column = column;
    this.columns = columns;
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
  public List<Record> execute(List<Record> records, PipelineContext context) throws DirectiveExecutionException {
    for (Record record : records) {
      JsonObject object = new JsonObject();
      for (String col : columns) {
        Object value = record.getValue(col);
        if (value instanceof Integer) {
          object.addProperty(col, (Integer) value);
        } else if (value instanceof Long) {
          object.addProperty(col, (Long) value);
        } else if (value instanceof Number) {
          object.addProperty(col, (Number) value);
        } else if (value instanceof Float) {
          object.addProperty(col, (Float) value);
        } else if (value instanceof Double) {
          object.addProperty(col, (Double) value);
        } else if (value instanceof Short) {
          object.addProperty(col, (Short) value);
        } else if (value instanceof Character) {
          object.addProperty(col, Character.toString((Character) value));
        } else if (value instanceof String) {
          object.addProperty(col, (String) value);
        } else if (value instanceof JsonElement) {
          object.add(col, (JsonElement) value);
        } else if (value instanceof JsonNull) {
          object.add(col, (JsonNull) value);
        }
      }
      record.addOrSet(column, object);
    }
    return records;
  }
}
