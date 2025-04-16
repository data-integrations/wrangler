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
import java.util.TreeMap;

/**
 * This class manages different measurements.
 */
public final class Measurements {
  // Measurement name to it's value mapping.
  private final Map<String, MutableDouble> metrics = new TreeMap<>();

  /**
   * Mutable Double for faster updates and checks.
   */
  private class MutableDouble {
    private double value = 0.0f;

    /**
     * Constructor to initialize with starting value.
     * @param value to be set.
     */
    MutableDouble(double value) {
      this.value = value;
    }

    /**
     * Increments the value associated with {@link MutableDouble}
     */
    public void increment() {
      ++value;
    }

    /**
     * @return value stored.
     */
    public double get() {
      return value;
    }
  }

  /**
   * Increment the {@link MutableDouble} value.
   *
   * @param name name of the measure who's value need to be incremented.
   */
  public void increment(String name) {
    MutableDouble value = metrics.get(name);
    if (value != null) {
      value.increment();
    } else {
      metrics.put(name, new MutableDouble(1));
    }
  }

  /**
   * Sets the measure value.
   *
   * @param name of the measure.
   * @param value to set for the measure.
   */
  public void set(String name, Double value) {
    metrics.put(name, new MutableDouble(value));
  }

  /**
   * Computes percentages for each of the measures managed by this instance.
   *
   * @param sum denominator for computing the percentages.
   * @return List of measures and associated percentages.
   */
  public List<Pair<String, Double>> percentage(Double sum) {
    List<Pair<String, Double>> percentages = new ArrayList<>();
    for (Map.Entry<String, MutableDouble> entry : metrics.entrySet()) {
      double percentage = entry.getValue().get() / sum;
      if (percentage > 100.0) {
        percentage = 100;
      }
      percentages.add(new Pair<>(entry.getKey(), percentage));
    }
    return percentages;
  }
}



