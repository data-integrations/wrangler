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

package co.cask.wrangler.api;

import co.cask.cdap.api.dataset.lib.KeyValue;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Record defines the schema and data on which the wrangler will operate upon.
 */
public class Record implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Record.class);

  // Values held by the row.
  private List<Object> values = new ArrayList<>();

  // Name of the columns held by the row.
  private List<String> columns = new ArrayList<>();

  public Record() {
  }

  /**
   * Makes a copy of the record.
   *
   * @param record to be copied to 'this' object.
   */
  public Record(Record record) {
    this.values = new ArrayList<>(record.values);
    this.columns = new ArrayList<>(record.columns);
  }

  /**
   * Initializes a row with list of columns.
   *
   * @param columns to set in the row.
   */
  public Record(List<String> columns) {
    this.columns = new ArrayList<>(columns);
  }

  /**
   * Initializes the row with column name and value.
   *
   * @param name of the column to be added to the row.
   * @param value for the column defined above.
   */
  public Record(String name, Object value) {
    this.columns = new ArrayList<>();
    this.values = new ArrayList<>();
    this.columns.add(name);
    this.values.add(value);
  }

  /**
   * Gets a column name by index.
   *
   * @param idx to retrieve the name of the column.
   * @return name of the column.
   */
  public String getColumn(int idx) {
    return columns.get(idx);
  }

  /**
   * Adds a column to the row.
   *
   * @param name of the column to be added.
   */
  public void addColumn(String name) {
    columns.add(name);
  }

  /**
   * Sets the name of the column at a given index.
   *
   * @param idx at which the new name to be set.
   * @param name of the column to be set at idx.
   */
  public void setColumn(int idx, String name) {
    columns.set(idx, name);
  }

  /**
   * Clears all the column entries for the row.
   */
  public void clearColumns() {
    columns.clear();
  }

  /**
   * Gets a value of row at specified index.
   *
   * @param idx from where the value should be retrieved.
   * @return value at index (idx).
   */
  public Object getValue(int idx) {
    return values.get(idx);
  }

  /**
   * Gets value based on the column name.
   *
   * @param col name of the column for which the value is retrieved.
   * @return value associated with column.
   */
  public Object getValue(String col) {
    if (col != null && !col.isEmpty()) {
      int idx = find(col);
      if (idx != -1) {
        return values.get(idx);
      }
    }
    return null;
  }

  /**
   * Adds a value to the row.
   *
   * @param value to be added to the row.
   */
  public Record addValue(Object value) {
    values.add(value);
    return this;
  }

  /**
   * Updates the value of the row at index idx.
   *
   * @param idx index at which the value needs to be updated.
   * @param value value to be updated at index (idx).
   */
  public Record setValue(int idx, Object value) {
    values.set(idx, value);
    return this;
  }

  /**
   * Adds a value into row with name.
   *
   * @param name of the value to be added to row.
   * @param value to be added to row.
   */
  public Record add(String name, Object value) {
    columns.add(name);
    values.add(value);
    return this;
  }

  /**
   * Removes the column and value at given index.
   *
   * @param idx for which the value and column are removed.
   */
  public Record remove(int idx) {
    columns.remove(idx);
    values.remove(idx);
    return this;
  }

  /**
   * Finds a column index based on the name of the column.
   *
   * @param col to be searched within the row.
   * @return null if not present, else the index at which the column is found.
   */
  public int find(String col) {
    int idx = 0;
    for (String name : columns) {
      if (col.equalsIgnoreCase(name)) {
        return idx;
      }
      idx++;
    }
    return -1;
  }

  /**
   * @return  Length of the row.
   */
  public int length() {
    return columns.size();
  }

  /**
   * @return List of fields of record.
   */
  public List<KeyValue<String, Object>> getFields() {
    List<KeyValue<String, Object>> v = new ArrayList<>();
    int i = 0;
    for (String column : columns) {
      v.add(new KeyValue<>(column, values.get(i)));
      ++i;
    }
    return v;
  }

  /**
   * Adds or sets the value.
   *
   * @param name of the field to be either set or added to record.
   * @param value to be added.
   */
  public void addOrSet(String name, Object value) {
    int idx = find(name);
    if (idx != -1) {
      setValue(idx, value);
    } else {
      add(name, value);
    }
  }

  public List<String> getColumns() {
    return ImmutableList.copyOf(columns);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
      .add("column-size", columns.size())
      .add("values-size", values.size())
      .add("columns", columns)
      .add("values", values)
      .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Record that = (Record) o;

    return Objects.equal(this.values, that.values) &&
      Objects.equal(this.columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(values, columns);
  }
}
