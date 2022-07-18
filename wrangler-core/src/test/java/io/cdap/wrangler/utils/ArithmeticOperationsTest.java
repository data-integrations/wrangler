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
