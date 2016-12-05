/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.steps;

import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.ColumnType;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.StepException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A Wrangle step that sets the {@link ColumnType} of {@link Row} columns.
 */
public class Types extends AbstractStep {
  private static final Logger LOG = LoggerFactory.getLogger(Columns.class);

  // List of types associated with columns in a {@link Row}.
  private List<ColumnType> types = new ArrayList<>();

  // Replaces the input {@link Row} column types.
  boolean replaceColumnTypes;

  public Types(int lineno, String detail, List<ColumnType> types) {
    this(lineno, detail, types, true);
  }

  public Types(int lineno, String detail, List<ColumnType> types, boolean replaceColumnTypes) {
    super(lineno, detail);
    this.replaceColumnTypes = true;
    this.types = types;
  }

  /**
   * Sets the types of columns in the input {@link Row}.
   *
   * @param row Input {@link Row} to be wrangled by this step.
   * @return A transformed row with column types added.
   * @throws StepException
   */
  @Override
  public Row execute(Row row) throws StepException {
    Row r = new Row(row);
    if (replaceColumnTypes) {
      r.clearTypes();
    }
    for (ColumnType type : types) {
      r.addType(type);
    }
    return r;
  }
}

