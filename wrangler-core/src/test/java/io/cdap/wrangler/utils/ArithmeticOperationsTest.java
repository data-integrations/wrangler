/*
 * Copyright © 2022 Cask Data, Inc.
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Tests {@link ArithmeticOperations}
 */
public class ArithmeticOperationsTest {
  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  private static final double DELTA = 0.0001;

  /**
   * Test add operation functionality
   *
   * @throws Exception
   */
  @Test
  public void testAdd() throws Exception {

    // Test 1 value
    Assert.assertEquals((short) 5, ArithmeticOperations.add((short) 5)); // short
    Assert.assertEquals(5, ArithmeticOperations.add(5)); // int
    Assert.assertEquals((long) 5, ArithmeticOperations.add((long) 5)); // long
    Assert.assertEquals((float) 5, ArithmeticOperations.add((float) 5)); // float
    Assert.assertEquals(5.0, ArithmeticOperations.add(5.0)); // double
    Assert.assertEquals(BigDecimal.valueOf(5),
                        ArithmeticOperations.add(BigDecimal.valueOf(5))); // BigDecimal

    // Test 3 values
    Assert.assertEquals((short) 4,
                        ArithmeticOperations.add((short) 1, (short) 3)); // short
    Assert.assertEquals(4, ArithmeticOperations.add(1, 3)); // int
    Assert.assertEquals((long) 4,
                        ArithmeticOperations.add((long) 1, (long) 3)); // long
    Assert.assertEquals((float) 4,
                        ArithmeticOperations.add((float) 1, (float) 3)); // float
    Assert.assertEquals(4.0, ArithmeticOperations.add(1.0, 3.0)); // double
    Assert.assertEquals(BigDecimal.valueOf(4),
                        ArithmeticOperations.add(BigDecimal.ONE,
                                                 BigDecimal.valueOf(3))); // BigDecimal

    // Test all null values
    Assert.assertNull(ArithmeticOperations.add(null, null));

    // Test one null value
    Assert.assertNull(ArithmeticOperations.add((short) 2, null, (short) 3)); // short
    Assert.assertNull(ArithmeticOperations.add(2, null, 3)); // int
    Assert.assertNull(ArithmeticOperations.add((long) 2, null, (long) 3)); // long
    Assert.assertNull(ArithmeticOperations.add((float) 2, null, (float) 3)); // float
    Assert.assertNull(ArithmeticOperations.add(2.0, null, 3.0)); // double
    Assert.assertNull(ArithmeticOperations.add(BigDecimal.valueOf(2),
                                               null,
                                               BigDecimal.valueOf(3))); // BigDecimal

    // Test multiple types (pairs)
    Assert.assertEquals(5,
                        ArithmeticOperations.add((short) 2, 2, (short) 1)); // short + int
    Assert.assertEquals((long) 5,
                        ArithmeticOperations.add(2, (long) 2, 1)); // int + long
    Assert.assertEquals((float) 5,
                        ArithmeticOperations.add((long) 2, (float) 2, (long) 1)); // long + float
    Assert.assertEquals((double) 5,
                        ArithmeticOperations.add((float) 2, 2.0, (float) 1)); // float + double
    Assert.assertEquals(BigDecimal.valueOf(5),
                        ArithmeticOperations.add(2.0, BigDecimal.valueOf(2), 1.0)); // double + BigDecimal

    // Test multiple types (all)
    Assert.assertEquals(BigDecimal.valueOf(21), ArithmeticOperations.add((short) 1,
                                                                         2,
                                                                         (long) 3,
                                                                         (float) 4,
                                                                         5.0,
                                                                         BigDecimal.valueOf(6)));
  }

  /**
   * Test minus (subtraction) operation functionality
   *
   * @throws Exception
   */
  @Test
  public void testMinus() throws Exception {

    // Test same type
    Assert.assertEquals((short) 2, ArithmeticOperations.minus((short) 10, (short) 8)); // Short
    Assert.assertEquals(2, ArithmeticOperations.minus(10, 8)); // Integer
    Assert.assertEquals((long) 2, ArithmeticOperations.minus((long) 10, (long) 8)); // Long
    Assert.assertEquals((float) 2.5, (float) ArithmeticOperations.minus((float) 10.7, (float) 8.2), DELTA); // Float
    Assert.assertEquals(2.5, (double) ArithmeticOperations.minus(10.7, 8.2), DELTA); // Double
    Assert.assertEquals(BigDecimal.valueOf(2.5), ArithmeticOperations.minus(BigDecimal.valueOf(10.7),
                                                                            BigDecimal.valueOf(8.2))); // BigDecimal

    // Test different types; expected behavior is to output the most "general" type
    Assert.assertEquals(2, ArithmeticOperations.minus((short) 10, 8)); // Short + Integer
    Assert.assertEquals((long) 2, ArithmeticOperations.minus(10,  (long) 8)); // Integer + Long
    Assert.assertEquals((float) 2, (float) ArithmeticOperations.minus((long) 10, (float) 8), DELTA); // Long + Float
    Assert.assertEquals(2.5, (double) ArithmeticOperations.minus((float) 10.7, 8.2), DELTA); // Float + Double
    Assert.assertEquals(BigDecimal.valueOf(2.5),
                        ArithmeticOperations.minus(10.7, BigDecimal.valueOf(8.2))); // Double + BigDecimal
  }

  /**
   * Test multiply operation multi-column functionality
   *
   * @throws Exception
   */
  @Test
  public void testMultiply() throws Exception {

    // Test 1 value
    Assert.assertEquals((short) 5, ArithmeticOperations.multiply((short) 5)); // short
    Assert.assertEquals(5, ArithmeticOperations.multiply(5)); // int
    Assert.assertEquals((long) 5, ArithmeticOperations.multiply((long) 5)); // long
    Assert.assertEquals((float) 5, ArithmeticOperations.multiply((float) 5)); // float
    Assert.assertEquals(5.0, ArithmeticOperations.multiply(5.0)); // double
    Assert.assertEquals(BigDecimal.valueOf(5),
                        ArithmeticOperations.multiply(BigDecimal.valueOf(5))); // BigDecimal

    // Test 3 values
    Assert.assertEquals((short) 3,
                        ArithmeticOperations.multiply((short) 1, (short) 3)); // short
    Assert.assertEquals(3, ArithmeticOperations.multiply(1, 3)); // int
    Assert.assertEquals((long) 3,
                        ArithmeticOperations.multiply((long) 1, (long) 3)); // long
    Assert.assertEquals((float) 3,
                        ArithmeticOperations.multiply((float) 1, (float) 3)); // float
    Assert.assertEquals(3.0, ArithmeticOperations.multiply(1.0, 3.0)); // double
    Assert.assertEquals(BigDecimal.valueOf(3),
                        ArithmeticOperations.multiply(BigDecimal.ONE,
                                                      BigDecimal.valueOf(3))); // BigDecimal

    // Test all null values
    Assert.assertNull(ArithmeticOperations.multiply(null, null));

    // Test one null value
    Assert.assertNull(ArithmeticOperations.multiply((short) 2, null, (short) 3)); // short
    Assert.assertNull(ArithmeticOperations.multiply(2, null, 3)); // int
    Assert.assertNull(ArithmeticOperations.multiply((long) 2, null, (long) 3)); // long
    Assert.assertNull(ArithmeticOperations.multiply((float) 2, null, (float) 3)); // float
    Assert.assertNull(ArithmeticOperations.multiply(2.0, null, 3.0)); // double
    Assert.assertNull(ArithmeticOperations.multiply(BigDecimal.valueOf(2),
                                                    null,
                                                    BigDecimal.valueOf(3))); // BigDecimal

    // Test multiple types (pairs)
    Assert.assertEquals(4,
                        ArithmeticOperations.multiply((short) 2, 2, (short) 1)); // short + int
    Assert.assertEquals((long) 4,
                        ArithmeticOperations.multiply(2, (long) 2, 1)); // int + long
    Assert.assertEquals((float) 4,
                        ArithmeticOperations.multiply((long) 2, (float) 2, (long) 1)); // long + float
    Assert.assertEquals((double) 4,
                        ArithmeticOperations.multiply((float) 2, 2.0, (float) 1)); // float + double
    Assert.assertEquals(new BigDecimal("4.00"),
                        ArithmeticOperations.multiply(2.0,
                                                      BigDecimal.valueOf(2),
                                                      1.0)); // double + BigDecimal

    // Test multiple types (all)
    Assert.assertEquals(new BigDecimal("720.00"),
                        ArithmeticOperations.multiply((short) 1,
                                                      2,
                                                      (long) 3,
                                                      (float) 4,
                                                      5.0,
                                                      BigDecimal.valueOf(6)));
  }

  /**
   * Test divideq (division) operation functionality
   *
   * @throws Exception
   */
  @Test
  public void testDivideq() throws Exception {

    // Test same type
    // Expected behavior is to output Double, unless any input has type BigDecimal
    Assert.assertEquals(2.4, (double) ArithmeticOperations.divideq((short) 12, (short) 5), DELTA); // Short
    Assert.assertEquals(2.4, (double) ArithmeticOperations.divideq(12, 5), DELTA); // Integer
    Assert.assertEquals(2.4, (double) ArithmeticOperations.divideq((long) 12, (long) 5), DELTA); // Long
    Assert.assertEquals(30.25, (double) ArithmeticOperations.divideq((float) 12.1, (float) 0.4),
                        DELTA); // Float
    Assert.assertEquals(30.25, (double) ArithmeticOperations.divideq(12.1, 0.4), DELTA); // Double
    Assert.assertEquals(BigDecimal.valueOf(30.3), ArithmeticOperations.divideq(BigDecimal.valueOf(9.09),
                                                                              BigDecimal.valueOf(0.3))); // BigDecimal

    // Test different types
    Assert.assertEquals(2.4, (double) ArithmeticOperations.divideq((short) 12, 5), DELTA); // Short + Integer
    Assert.assertEquals(2.4, (double) ArithmeticOperations.divideq(12,  (long) 5), DELTA); // Integer + Long
    Assert.assertEquals(2.4, (double) ArithmeticOperations.divideq((long) 12,
                                                                          (float) 5.0), DELTA); // Long + Float
    Assert.assertEquals(30.25, (double) ArithmeticOperations.divideq((float) 12.1, 0.4),
                         DELTA); // Float + Double
    Assert.assertEquals(BigDecimal.valueOf(30.3),
                        ArithmeticOperations.divideq(9.09, BigDecimal.valueOf(0.3))); // Double + BigDecimal

    // Test division by 0
    Assert.assertEquals(null, ArithmeticOperations.divideq(5, 0)); // Integer 0
    Assert.assertEquals(null, ArithmeticOperations.divideq(5, (double) 0)); // Double 0
    Assert.assertEquals(null, ArithmeticOperations.divideq(5, BigDecimal.valueOf(0))); // BigDecimal 0
  }

  /**
   * Test divider (modulus) operation functionality
   *
   * @throws Exception
   */
  @Test
  public void testDivider() throws Exception {

    // Test same type
    Assert.assertEquals(2, ArithmeticOperations.divider((short) 12, (short) 5)); // Short
    Assert.assertEquals(2, ArithmeticOperations.divider(12, 5)); // Integer
    Assert.assertEquals((long) 2, ArithmeticOperations.divider((long) 12, (long) 5)); // Long
    Assert.assertEquals((float) 0.2, (float) ArithmeticOperations.divider((float) 12.2, (float) 0.5), DELTA); // Float
    Assert.assertEquals(0.2, (double) ArithmeticOperations.divider(12.2, 0.5), DELTA); // Double
    Assert.assertEquals(BigDecimal.valueOf(0.2), ArithmeticOperations.divider(BigDecimal.valueOf(12.2),
                                                                             BigDecimal.valueOf(0.5))); // BigDecimal

    // Test different types; expected behavior is to output the most "general" type
    Assert.assertEquals(2, ArithmeticOperations.divider((short) 12, 5)); // Short + Integer
    Assert.assertEquals((long) 2, ArithmeticOperations.divider(12,  (long) 5)); // Integer + Long
    Assert.assertEquals((float) 2.0, (float) ArithmeticOperations.divider((long) 12,
                                                                          (float) 5.0), DELTA); // Long + Float
    Assert.assertEquals(0.2, (double) ArithmeticOperations.divider((float) 12.2, 0.5), DELTA); // Float + Double
    Assert.assertEquals(BigDecimal.valueOf(0.2),
                        ArithmeticOperations.divider(12.2, BigDecimal.valueOf(0.5))); // Double + BigDecimal

    // Test division by 0
    Assert.assertEquals(null, ArithmeticOperations.divider(5, 0)); // Integer 0
    Assert.assertEquals(null, ArithmeticOperations.divider(5, (double) 0)); // Double 0
    Assert.assertEquals(null, ArithmeticOperations.divider(5, BigDecimal.valueOf(0))); // BigDecimal 0
  }

  /**
   * Test LCM operation functionality
   *
   * @throws Exception
   */
  @Test
  public void testLCM() throws Exception {

    // Test same type
    Assert.assertEquals((short) 36, ArithmeticOperations.lcm((short) 12, (short) 18)); // Short
    Assert.assertEquals(36, ArithmeticOperations.lcm(12, 18)); // Integer
    Assert.assertEquals((long) 36, ArithmeticOperations.lcm((long) 12, (long) 18)); // Long
    Assert.assertEquals((float) 2262.5, ArithmeticOperations.lcm((float) 12.5, (float) 18.1)); // Float
    Assert.assertEquals(2262.5, ArithmeticOperations.lcm(12.5, 18.1)); // Double
    Assert.assertEquals(BigDecimal.valueOf(2262.5), ArithmeticOperations.lcm(BigDecimal.valueOf(12.5),
                                                                             BigDecimal.valueOf(18.1))); // BigDecimal

    // Test different types; expected behavior is to output the most "general" type
    Assert.assertEquals(36, ArithmeticOperations.lcm((short) 12, 18)); // Short + Integer
    Assert.assertEquals((long) 36, ArithmeticOperations.lcm(12,  (long) 18)); // Integer + Long
    Assert.assertEquals((float) 36.0, ArithmeticOperations.lcm((long) 12,  (float) 18.0)); // Long + Float
    Assert.assertEquals(2262.5, ArithmeticOperations.lcm((float) 12.5, 18.1)); // Float + Double
    Assert.assertEquals(BigDecimal.valueOf(2262.5),
                         ArithmeticOperations.lcm(12.5, BigDecimal.valueOf(18.1))); // Double + BigDecimal
  }

  /**
   * Test equal operation functionality
   *
   * @throws Exception
   */
  @Test
  public void testEqual() throws Exception {

    // Test 1 value
    Assert.assertTrue(ArithmeticOperations.equal(3));

    // Test 3 of the same value
    Assert.assertTrue(ArithmeticOperations.equal(3, 3, 3));

    // Test different values
    Assert.assertFalse(ArithmeticOperations.equal(2, 4));

    // Test null value
    Assert.assertNull(ArithmeticOperations.equal((Integer) null));

    // Test multiple null values
    Assert.assertNull(ArithmeticOperations.equal((Integer) null, null));

    // Test embedded null value
    Assert.assertNull(ArithmeticOperations.equal(3, null, 3));

    // Test precision
    Assert.assertFalse(ArithmeticOperations.equal(BigDecimal.ONE, BigDecimal.valueOf(1.0)));
  }

  /**
   * Test max operation functionality
   *
   * @throws DirectiveExecutionException
   */
  @Test
  public void testMax() throws DirectiveExecutionException {

    // Test 1 value
    Assert.assertEquals(2.0, ArithmeticOperations.max(2.0));

    // Test 3 of the same value
    Assert.assertEquals(2.0, ArithmeticOperations.max(2.0, 2.0, 2.0));

    // Test at beginning
    Assert.assertEquals(4.0, ArithmeticOperations.max(4.0, 2.0, 3.5));

    // Test in middle
    Assert.assertEquals(5.0, ArithmeticOperations.max(4.0, 5.0, 3.5));

    // Test at end
    Assert.assertEquals(3.0, ArithmeticOperations.max(2.0, 1.0, 3.0));

    // Test negative numbers
    Assert.assertEquals(-1.0, ArithmeticOperations.max(-5.0, -1.0, -2.0));

    // Test cross-type
    Assert.assertEquals((long) 5, ArithmeticOperations.max(5, (long) 3, 5));
    Assert.assertEquals((float) 4, ArithmeticOperations.max((long) 4,
                                                                      (float) 2,
                                                                      (long) 4));
    Assert.assertEquals(3.0, ArithmeticOperations.max((float) 3, 2.0, (float) 3));
    Assert.assertEquals(BigDecimal.valueOf(2.0), ArithmeticOperations.max(2.0,
                                                                          BigDecimal.valueOf(1.0),
                                                                          2.0));

    // Test overall cross-type
    Assert.assertEquals(BigDecimal.valueOf(5), ArithmeticOperations.max(5,
                                                                          (long) 4,
                                                                          (float) 3,
                                                                          2.0,
                                                                          BigDecimal.valueOf(1.0)));
  }

  /**
   * Tests for max operation exception with Byte input
   *
   * @throws DirectiveExecutionException
   */
  @Test
  public void testMaxByteException() throws DirectiveExecutionException {
    expectedEx.expect(Exception.class);
    ArithmeticOperations.max(Byte.valueOf((byte) 0));
  }

  /**
   * Test min operation functionality
   *
   * @throws DirectiveExecutionException
   */
  @Test
  public void testMin() throws DirectiveExecutionException {

    // Test 1 value
    Assert.assertEquals(2.0, ArithmeticOperations.min(2.0));

    // Test 3 of the same value
    Assert.assertEquals(2.0, ArithmeticOperations.min(2.0, 2.0, 2.0));

    // Test at beginning
    Assert.assertEquals(2.0, ArithmeticOperations.min(2.0, 4.0, 3.5));

    // Test in middle
    Assert.assertEquals(3.5, ArithmeticOperations.min(4.0, 3.5, 5.0));

    // Test at end
    Assert.assertEquals(1.0, ArithmeticOperations.min(2.0, 3.0, 1.0));

    // Test negative numbers
    Assert.assertEquals(-5.0, ArithmeticOperations.min(-5.0, -1.0, -2.0));

    // Test cross-type
    Assert.assertEquals((long) 3, ArithmeticOperations.min(3, (long) 5, 3));
    Assert.assertEquals((float) 4, ArithmeticOperations.min((long) 4,
                                                                      (float) 5,
                                                                      (long) 4));
    Assert.assertEquals(2.0, ArithmeticOperations.min((float) 2, 3.0, (float) 2));
    Assert.assertEquals(BigDecimal.valueOf(1.0), ArithmeticOperations.min(1.0,
                                                                 BigDecimal.valueOf(2.0),
                                                                 1.0));

    // Test overall cross-type
    Assert.assertEquals(BigDecimal.valueOf(1.0), ArithmeticOperations.min(5,
                                                                 (long) 4,
                                                                 (float) 3,
                                                                 2.0,
                                                                 BigDecimal.valueOf(1.0)));
  }

  /**
   * Tests for min operation exception with Byte input
   *
   * @throws DirectiveExecutionException
   */
  @Test
  public void testMinByteException() throws DirectiveExecutionException {
    expectedEx.expect(Exception.class);
    ArithmeticOperations.min(Byte.valueOf((byte) 0));
  }

  /**
   * Test average operation functionality.
   *
   * @throws Exception
   */
  @Test
  public void testAverage() throws Exception {

    // Test 1 value
    Assert.assertEquals(5.0, ArithmeticOperations.average(5.0));

    // Test 3 values
    Assert.assertEquals(2.0, ArithmeticOperations.average(1.0, 2.0, 3.0));

    // Test repeating decimal
    Assert.assertEquals(1.0 / 3.0,
                        ArithmeticOperations.average(1.0, 0.0, 0.0).doubleValue(),
                        0.0001);

    // Test null value
    Assert.assertEquals(2.0, ArithmeticOperations.average(null, 2));

    // Test all null values
    Assert.assertNull(ArithmeticOperations.average(null, null));

    // Test multiple data types (no BigDecimal)
    Assert.assertEquals(3.0, ArithmeticOperations.average(1,
                                                          (long) 2,
                                                          (short) 3,
                                                          (float) 4,
                                                          (double) 5));

    // Test 1 value (BigDecimal)
    Assert.assertEquals(BigDecimal.valueOf(5),
                        ArithmeticOperations.average(BigDecimal.valueOf(5)));

    // Test 3 values (BigDecimal)
    Assert.assertEquals(BigDecimal.valueOf(2.0),
                        ArithmeticOperations.average(BigDecimal.valueOf(1.0),
                                                     BigDecimal.valueOf(2.0),
                                                     BigDecimal.valueOf(3.0)));

    // Test repeating decimal (BigDecimal)
    Assert.assertEquals(BigDecimal.valueOf(1.0).divide(BigDecimal.valueOf(3), RoundingMode.HALF_EVEN),
                        ArithmeticOperations.average(BigDecimal.valueOf(1.0),
                                                     BigDecimal.ZERO,
                                                     BigDecimal.ZERO));

    // Test multiple data types (BigDecimal)
    Assert.assertEquals(BigDecimal.valueOf(3.0),
                        ArithmeticOperations.average(2,
                                                     (long) 2,
                                                     (short) 2,
                                                     (float) 4,
                                                     (double) 4,
                                                     BigDecimal.valueOf(4)));
  }

  /**
   * Tests for average operation exception with Byte input
   *
   * Input: (byte) 0
   * Expected: Exception
   *
   * @throws Exception
   */
  @Test
  public void testAverageByteException() throws Exception {
    expectedEx.expect(Exception.class);
    ArithmeticOperations.average(Byte.valueOf((byte) 0));
  }
}
