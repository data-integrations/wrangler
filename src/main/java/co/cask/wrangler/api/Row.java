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

package co.cask.wrangler.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by nitin on 12/3/16.
 */
public class Row {
  private static final Logger LOG = LoggerFactory.getLogger(Row.class);

  private List<Object> values = new ArrayList<>();
  private List<ColumnType> types = new ArrayList<>();
  private List<String> columns = new ArrayList<>();

  public Row() {}

  public Row(Row row) {
    this.values = row.values;
    this.types = row.types;
    this.columns = row.columns;
  }

  public Row(List<String> columns) {
    this.columns = columns;
  }

  public Row(List<String> columns, List<ColumnType> types) {
    this.types = types;
    this.columns = columns;
  }

  public Row(String name, ColumnType type, Object value) {
    this.columns.add(name);
    this.types.add(type);
    this.values.add(value);
  }

  public String getName(int idx) {
    return columns.get(idx);
  }

  public ColumnType getType(int idx) {
    return types.get(idx);
  }

  public Object get(String col) {
    if (col != null && !col.isEmpty()) {
      int idx = 0;
      for (String name : columns) {
        if (col.equalsIgnoreCase(name)) {
          return values.get(idx);
        }
        idx++;
      }
    }
    return null;
  }

  public String getString(int idx) {
    if (types.get(idx) == ColumnType.STRING) {
      return (String)values.get(idx);
    }
    return null;
  }

  public String getString(String col) {
    int idx = find(col);
    if (idx != -1) {
      if (types.get(idx) == ColumnType.STRING) {
        return (String)values.get(idx);
      }
    }
    return null;
  }

  public void addValue(Object value) {
    values.add(value);
  }

  public void setValue(int idx, Object value) {
    values.set(idx, value);
  }

  public void addType(ColumnType type) {
    types.add(type);
  }

  public void addName(String name) {
    columns.add(name);
  }

  public void setName(int idx, String name) {
    columns.set(idx, name);
  }

  public void add(String name, ColumnType type, Object value) {
    columns.add(name);
    types.add(type);
    values.add(value);
  }

  public void clearTypes() {
    types.clear();
  }

  public void clearColumns() {
    columns.clear();
  }

  public void remove(int idx) {
    columns.remove(idx);
    values.remove(idx);
    types.remove(idx);
  }

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

  public int length() {
    return columns.size();
  }

  public Object getValue(int idx) {
    return values.get(idx);
  }
}
