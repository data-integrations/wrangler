/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.statistics;

import io.cdap.wrangler.api.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class stores and manages {@link Measurements} for each column.
 */
public final class ColumnMetric {
  private final Map<String, Measurements> measures = new TreeMap<>();

  /**
   * Increments a measure counter for a column.
   *
   * @param column name of the column
   * @param measure to be incremented for a column.
   */
  public void increment(String column, String measure) {
    Measurements metric;
    if (measures.containsKey(column)) {
      metric = measures.get(column);
    } else {
      metric = new Measurements();
    }
    metric.increment(measure);
    measures.put(column, metric);
  }

  /**
   * Sets the measure value for a column.
   *
   * @param column name of the column.
   * @param measure name of the measure that needs to be set for the column.
   * @param value to be set for the measure.
   */
  public void set(String column, String measure, double value) {
    Measurements metric;
    if (measures.containsKey(column)) {
      metric = measures.get(column);
    } else {
      metric = new Measurements();
    }
    metric.set(measure, value);
    measures.put(column, metric);
  }

  /**
   * @return Set of columns tracked.
   */
  public Set<String> getColumns() {
    return measures.keySet();
  }

  /**
   * Computes percentages for each measure for a given column.
   *
   * @param column name of the column.
   * @param sum denonminator for calculating percentages.
   * @return List of measures and their respective percentages.
   */
  public List<Pair<String, Double>> percentage(String column, Double sum) {
    if (measures.containsKey(column)) {
      return measures.get(column).percentage(sum);
    }
    return new ArrayList<>();
  }
}
