/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package io.cdap.wrangler.dq;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link ConvertDistances}
 */
public class ConvertDistancesTest {

  private double delta = 1.0E-34;

  @Test
  public void testConvertDoubleNan() {
    double nan = Double.NaN;
    ConvertDistances converter = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                      ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(nan, converter.convert(nan), delta);
  }

  @Test
  public void testConvertZero() {
    double zero = 0.0;
    ConvertDistances converter = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                      ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(zero, converter.convert(zero), delta);
  }

  @Test
  public void testConvertMaxValue() {
    double max = Double.MAX_VALUE;
    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(Double.POSITIVE_INFINITY, converter0.convert(max), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(1.900163142869793E289, converter1.convert(max), delta);
  }

  @Test
  public void testConvertMinValue() {
    double min = Double.MIN_VALUE;
    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(0.0, converter0.convert(min), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(0.0, converter1.convert(min), delta);
  }

  @Test
  public void testConvertDefault() {
    double mi = 1.0;
    double km = 1.609344;

    ConvertDistances converter = new ConvertDistances();
    Assert.assertEquals(km, converter.convert(mi), delta);
  }

  @Test
  public void testConvertMillimeter() {
    double mm = 1.0;
    double cm = 0.1;
    double dm = 0.01;
    double m = 0.001;
    double dam = 0.0001;
    double hm = 0.00001;
    double km = 0.000001;
    double in = 0.03937007874015748;
    double ft = 0.0032808398950131233;
    double yd = 0.0010936132983377078;
    double mi = 6.213711922373339E-7;
    double nm = 5.399568034557235E-7;
    double ly = 1.0570008340246155E-19;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter0.convert(mm), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter1.convert(mm), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter2.convert(mm), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter3.convert(mm), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter4.convert(mm), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(mm), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter6.convert(mm), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(mm), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(mm), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(mm), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(mm), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(mm), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.MILLIMETER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(mm), delta);
  }

  @Test
  public void testConvertCentimeter() {
    double cm = 1.0;
    double mm = 10.0;
    double dm = 0.1;
    double m = 0.01;
    double dam = 0.001;
    double hm = 0.0001;
    double km = 0.00001;
    double in = 0.3937007874015748;
    double ft = 0.032808398950131233;
    double yd = 0.010936132983377077;
    double mi = 6.213711922373339E-6;
    double nm = 5.399568034557236E-6;
    double ly = 1.0570008340246153E-18;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter0.convert(cm), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(cm), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter2.convert(cm), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter3.convert(cm), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter4.convert(cm), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(cm), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter6.convert(cm), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(cm), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(cm), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(cm), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(cm), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(cm), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.CENTIMETER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(cm), delta);
  }

  @Test
  public void testConvertDecimeter() {
    double dm = 1.0;
    double mm = 100.0;
    double cm = 10.0;
    double m = 0.1;
    double dam = 0.01;
    double hm = 0.001;
    double km = 0.0001;
    double in = 3.937007874015748;
    double ft = 0.32808398950131233;
    double yd = 0.10936132983377078;
    double mi = 6.213711922373339E-5;
    double nm = 5.399568034557236E-5;
    double ly = 1.0570008340246154E-17;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter0.convert(dm), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(dm), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(dm), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter3.convert(dm), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter4.convert(dm), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(dm), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter6.convert(dm), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(dm), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(dm), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(dm), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(dm), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(dm), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.DECIMETER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(dm), delta);
  }

  @Test
  public void testConvertMeter() {
    double m = 1.0;
    double mm = 1000.0;
    double cm = 100.0;
    double dm = 10.0;
    double dam = 0.1;
    double hm = 0.01;
    double km = 0.001;
    double in = 39.37007874015748;
    double ft = 3.2808398950131235;
    double yd = 1.0936132983377078;
    double mi = 6.213711922373339E-4;
    double nm = 5.399568034557236E-4;
    double ly = 1.0570008340246154E-16;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter0.convert(m), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(m), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(m), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(m), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter4.convert(m), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(m), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter6.convert(m), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(m), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(m), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(m), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(m), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(m), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.METER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(m), delta);
  }

  @Test
  public void testConvertDekameter() {
    double dam = 1.0;
    double mm = 10000.0;
    double cm = 1000.0;
    double dm = 100.0;
    double m = 10;
    double hm = 0.1;
    double km = 0.01;
    double in = 393.7007874015748;
    double ft = 32.808398950131235;
    double yd = 10.936132983377078;
    double mi = 0.006213711922373339;
    double nm = 0.005399568034557236;
    double ly = 1.0570008340246154E-15;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter0.convert(dam), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(dam), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(dam), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(dam), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(dam), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(dam), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter6.convert(dam), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(dam), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(dam), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(dam), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(dam), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(dam), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.DEKAMETER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(dam), delta);
  }

  @Test
  public void testConvertHectometer() {
    double hm = 1.0;
    double mm = 100000.0;
    double cm = 10000.0;
    double dm = 1000.0;
    double m = 100.0;
    double dam = 10.0;
    double km = 0.1;
    double in = 3937.007874015748;
    double ft = 328.0839895013124;
    double yd = 109.36132983377078;
    double mi = 0.06213711922373339;
    double nm = 0.05399568034557236;
    double ly = 1.0570008340246154E-14;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter0.convert(hm), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(hm), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(hm), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(hm), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(hm), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter5.convert(hm), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter6.convert(hm), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(hm), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(hm), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(hm), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(hm), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(hm), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.HECTOMETER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(hm), delta);
  }

  @Test
  public void testConvertKilometer() {
    double km = 1.0;
    double mm = 1000000.0;
    double cm = 100000.0;
    double dm = 10000.0;
    double m = 1000.0;
    double dam = 100.0;
    double hm = 10.0;
    double in = 39370.07874015748;
    double ft = 3280.8398950131236;
    double yd = 1093.6132983377078;
    double mi = 0.6213711922373339;
    double nm = 0.5399568034557236;
    double ly = 1.0570008340246153E-13;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter0.convert(km), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(km), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(km), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(km), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(km), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(km), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(km), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter7.convert(km), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(km), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(km), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(km), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(km), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.KILOMETER,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(km), delta);
  }

  @Test
  public void testConvertInch() {
    double in = 1.0;
    double mm = 25.4;
    double cm = 2.54;
    double dm = 0.254;
    double m = 0.0254;
    double dam = 0.00254;
    double hm = 0.000254;
    double km = 2.54E-5;
    double ft = 0.08333333333333334;
    double yd = 0.02777777777777778;
    double mi = 1.578282828282828E-5;
    double nm = 1.371490280777538E-5;
    double ly = 2.684782118422523E-18;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter0.convert(in), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(in), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(in), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(in), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(in), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(in), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(in), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter7.convert(in), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter8.convert(in), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(in), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(in), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(in), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.INCH,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(in), delta);
  }

  @Test
  public void testConvertFoot() {
    double ft = 1.0;
    double mm = 304.8;
    double cm = 30.48;
    double dm = 3.048;
    double m = 0.3048;
    double dam = 0.03048;
    double hm = 0.003048;
    double km = 3.048E-4;
    double in = 12;
    double yd = 0.3333333333333333;
    double mi = 1.8939393939393937E-4;
    double nm = 1.6457883369330455E-4;
    double ly = 3.221738542107028E-17;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter0.convert(ft), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(ft), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(ft), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(ft), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(ft), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(ft), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(ft), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter7.convert(ft), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter8.convert(ft), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter9.convert(ft), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(ft), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(ft), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.FOOT,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(ft), delta);
  }

  @Test
  public void testConvertYard() {
    double yd = 1.0;
    double mm = 914.4;
    double cm = 91.44;
    double dm = 9.144;
    double m = 0.9144;
    double dam = 0.09144;
    double hm = 0.009144;
    double km = 0.0009144;
    double in = 36.0;
    double ft = 3.0;
    double mi = 5.681818181818182E-4;
    double nm = 4.937365010799136E-4;
    double ly = 9.665215626321083E-17;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter0.convert(yd), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(yd), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(yd), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(yd), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(yd), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(yd), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(yd), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter7.convert(yd), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter8.convert(yd), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter9.convert(yd), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter10.convert(yd), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(yd), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.YARD,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(yd), delta);
  }

  @Test
  public void testConvertMile() {
    double mi = 1.0;
    double mm = 1609344;
    double cm = 160934.4;
    double dm = 16093.44;
    double m = 1609.344;
    double dam = 160.9344;
    double hm = 16.09344;
    double km = 1.609344;
    double in = 63360.0;
    double ft = 5280.0;
    double yd = 1760.0;
    double nm = 0.868976241900648;
    double ly = 1.7010779502325107E-13;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter0.convert(mi), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(mi), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(mi), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(mi), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(mi), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(mi), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(mi), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter7.convert(mi), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter8.convert(mi), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter9.convert(mi), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                        ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter10.convert(mi), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter11.convert(mi), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.MILE,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(mi), delta);
  }

  @Test
  public void testConvertNauticalMile() {
    double nm = 1.0;
    double mm = 1852000.0;
    double cm = 185200.0;
    double dm = 18520.0;
    double m = 1852.0;
    double dam = 185.2;
    double hm = 18.52;
    double km = 1.852;
    double in = 72913.38582677166;
    double ft = 6076.115485564304;
    double yd = 2025.3718285214347;
    double mi = 1.1507794480235425;
    double ly = 1.9575655446135877E-13;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter0.convert(nm), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(nm), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(nm), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(nm), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(nm), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(nm), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(nm), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter7.convert(nm), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter8.convert(nm), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter9.convert(nm), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                        ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter10.convert(nm), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter11.convert(nm), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.NAUTICAL_MILE,
                                                        ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter12.convert(nm), delta);
  }

  @Test
  public void testConvertLightYear() {
    double ly = 1.0;
    double mm = 9.4607304725808E18;
    double cm = 9.4607304725808E17;
    double dm = 9.4607304725808E16;
    double m = 9.4607304725808E15;
    double dam = 9.4607304725808E14;
    double hm = 9.4607304725808E13;
    double km = 9.4607304725808E12;
    double in = 3.7246970364491341E17;
    double ft = 3.1039141970409452E16;
    double yd = 1.034638065680315E16;
    double mi = 5.878625373183607E12;
    double nm = 5.108385784330886E12;

    ConvertDistances converter0 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.LIGHT_YEAR);
    Assert.assertEquals(ly, converter0.convert(ly), delta);
    ConvertDistances converter1 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.MILLIMETER);
    Assert.assertEquals(mm, converter1.convert(ly), delta);
    ConvertDistances converter2 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.CENTIMETER);
    Assert.assertEquals(cm, converter2.convert(ly), delta);
    ConvertDistances converter3 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.DECIMETER);
    Assert.assertEquals(dm, converter3.convert(ly), delta);
    ConvertDistances converter4 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.METER);
    Assert.assertEquals(m, converter4.convert(ly), delta);
    ConvertDistances converter5 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.HECTOMETER);
    Assert.assertEquals(hm, converter5.convert(ly), delta);
    ConvertDistances converter6 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.DEKAMETER);
    Assert.assertEquals(dam, converter6.convert(ly), delta);
    ConvertDistances converter7 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.KILOMETER);
    Assert.assertEquals(km, converter7.convert(ly), delta);
    ConvertDistances converter8 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.INCH);
    Assert.assertEquals(in, converter8.convert(ly), delta);
    ConvertDistances converter9 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                       ConvertDistances.Distance.FOOT);
    Assert.assertEquals(ft, converter9.convert(ly), delta);
    ConvertDistances converter10 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                        ConvertDistances.Distance.YARD);
    Assert.assertEquals(yd, converter10.convert(ly), delta);
    ConvertDistances converter11 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                        ConvertDistances.Distance.MILE);
    Assert.assertEquals(mi, converter11.convert(ly), delta);
    ConvertDistances converter12 = new ConvertDistances(ConvertDistances.Distance.LIGHT_YEAR,
                                                        ConvertDistances.Distance.NAUTICAL_MILE);
    Assert.assertEquals(nm, converter12.convert(ly), delta);
  }
}
