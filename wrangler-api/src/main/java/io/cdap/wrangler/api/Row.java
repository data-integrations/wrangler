/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.wrangler.api;

import java.util.List;

/**
 * Interface representing a row of data.
 */
public interface Row {
  /**
   * Gets the value at the specified index.
   */
  Object getValue(int idx);

  /**
   * Gets the column name at the specified index.
   */
  String getColumn(int idx);

  /**
   * Sets the column value at the specified index.
   */
  void setColumn(int idx, String name);

  /**
   * Adds or sets a value with the specified column name.
   */
  void addOrSet(String name, Object value);

  /**
   * Removes the column at the specified index.
   */
  void remove(int idx);

  /**
   * Finds the index of the specified column name.
   */
  int find(String name);

  /**
   * Gets the width (number of columns) of the row.
   */
  int width();

  /**
   * Gets all fields in the row.
   */
  List<Pair<String, Object>> getFields();
}
