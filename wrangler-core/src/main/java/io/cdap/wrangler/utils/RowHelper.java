/*
 * Copyright © 2023 Cask Data, Inc.
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

import io.cdap.wrangler.api.Row;

import java.util.List;

/**
 * Utility methods for {@link Row}
 */
public class RowHelper {
  private RowHelper() {
    throw new AssertionError("Cannot instantiate a static utility class");
  }

  /**
   * Creates a merged record after iterating through all rows.
   *
   * @param rows list of all rows.
   * @return A single record will rows merged across all columns.
   */
  public static Row createMergedRow(List<Row> rows) {
    Row merged = new Row();
    for (Row row : rows) {
      for (int i = 0; i < row.width(); ++i) {
        Object o = row.getValue(i);
        if (o != null) {
          int idx = merged.find(row.getColumn(i));
          if (idx == -1) {
            merged.add(row.getColumn(i), o);
          }
        }
      }
    }
    return merged;
  }
}
