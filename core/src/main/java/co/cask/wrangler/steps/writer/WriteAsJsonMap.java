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
import co.cask.wrangler.api.AbstractDirective;
import co.cask.wrangler.api.Pair;
import co.cask.wrangler.api.RecipeContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.annotations.Usage;
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
public class WriteAsJsonMap extends AbstractDirective {
  private final String column;
  private final Gson gson;

  public WriteAsJsonMap(int lineno, String directive, String column) {
    super(lineno, directive);
    this.column = column;
    this.gson = new Gson();
  }

  /**
   * Executes a wrangle step on single {@link Row} and return an array of wrangled {@link Row}.
   *
   * @param rows  Input {@link Row} to be wrangled by this step.
   * @param context {@link RecipeContext} passed to each step.
   * @return Wrangled {@link Row}.
   */
  @Override
  public List<Row> execute(List<Row> rows, RecipeContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      Map<String, Object> toJson = new HashMap<>();
      for (Pair<String, Object> entry : row.getFields()) {
        toJson.put(entry.getFirst(), entry.getSecond());
      }
      row.addOrSet(column, gson.toJson(toJson));
    }
    return rows;
  }
}
