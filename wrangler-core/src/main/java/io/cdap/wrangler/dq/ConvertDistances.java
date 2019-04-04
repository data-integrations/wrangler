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

package io.cdap.wrangler.dq;

import java.math.BigDecimal;
import javax.annotation.Nullable;

/**
 * Class for converting distances from one to another.
 */
public final class ConvertDistances {

  /**
   * Specifies different distances that can be converted.
   */
  public enum Distance {
    MILLIMETER("millimeter", "mm", 0.001, 1000),
    CENTIMETER("centimeter", "cm", 0.01, 100),
    DECIMETER("decimeter", "dm", 0.1, 10),
    METER("meter", "m", 1.0, 1.0),
    DEKAMETER("dekameter", "dam", 10.0, 0.1),
    HECTOMETER("hectometer", "hm", 100.0, 0.01),
    KILOMETER("kilometer", "km", 1000.0, 0.001),
    INCH("inch", "in", 0.0254, 39.3700787401574803),
    FOOT("foot", "ft", 0.3048, 3.28083989501312336),
    YARD("yard", "yd", 0.9144, 1.09361329833770779),
    MILE("mile", "mi", 1609.344, 0.00062137119223733397),
    NAUTICAL_MILE("nautical mile", "nm", 1852.0, 0.000539956803455723542),
    LIGHT_YEAR("light-year", "ly", 9460730472580800.0, 0.0000000000000001057000834024615463709);

    // Name of the distance -- display name.
    private String name;

    // Measure name
    private String measure;

    // Multiplier to the base.
    private double toBase;

    // Multipler from the base
    private double fromBase;

    Distance(String name, String measure, double toBase, double fromBase) {
      this.name = name;
      this.measure = measure;
      this.toBase = toBase;
      this.fromBase = fromBase;
    }

    /**
     * @return Display name of the distance.
     */
    public String getName() {
      return name;
    }

    /**
     * @return Meausre name of the distance.
     */
    public String getMeasureName() {
      return measure;
    }

    /**
     * @return Multiplier to be applied when converting to base.
     */
    public double getToBase() {
      return toBase;
    }

    /**
     * @return Multiplier to be applied when converting from base.
     */
    public double getFromBase() {
      return fromBase;
    }
  }

  // Distance to be converted from.
  private Distance from;

  // Distance to be converted to.
  private Distance to;

  // Multiplier for converting 'from' to 'to'
  private BigDecimal multiplier;

  public ConvertDistances() {
    this(Distance.MILE, Distance.KILOMETER);
  }

  @Nullable
  public ConvertDistances(Distance from, Distance to) {
    this.from = (from == null ? Distance.MILE : from);
    this.to = (to == null ? Distance.KILOMETER : to);
    this.multiplier = new BigDecimal(String.valueOf(this.from.getToBase()))
      .multiply(new BigDecimal(String.valueOf(this.to.getFromBase())));
  }

  /**
   * Applies the distance conversion.
   *
   * @param value to be converted from 'from' distance to 'to' distance.
   * @return converted value.
   */
  public double convert(double value) {
    if (Double.isNaN(value)) {
      return value;
    }

    if (from.equals(to)) {
      return value;
    }

    BigDecimal result = new BigDecimal(String.valueOf(value)).multiply(multiplier);
    return result.doubleValue();
  }
}
