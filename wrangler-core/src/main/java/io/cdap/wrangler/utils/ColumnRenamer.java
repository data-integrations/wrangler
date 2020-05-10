/*
 * Copyright © 2020 Cask Data, Inc.
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
package io.cdap.wrangler.utils;

import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.Row;

/**
 * Utility class that converts a {@link Row} column into another column.
 */
public abstract class ColumnRenamer {
  /**
   * Renames a column.
   *
   * @param row source record to be modified.
   * @param column name of the column within source record.
   * @param toName the target name of the column.
   * @throws DirectiveExecutionException when a column matching the target name already exists.
   */
  protected void rename(String directiveName, Row row, String column, String toName)
    throws DirectiveExecutionException {
    int idx = row.find(column);
    int existingColumn = row.find(toName);
    if (idx != -1) {
      if (existingColumn == -1) {
        row.setColumn(idx, toName);
      } else {
        throw new DirectiveExecutionException(
          directiveName, String.format("Column '%s' already exists. Apply the 'drop %s' directive before " +
                                         "renaming '%s' to '%s'.",
                                       toName, toName, column, toName));
      }
    }
  }
}
