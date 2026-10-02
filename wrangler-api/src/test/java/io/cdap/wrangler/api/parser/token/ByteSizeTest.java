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

package io.cdap.wrangler.api.parser.token;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize} class.
 */
public class ByteSizeTest {
  // Constants for byte conversions
  private static final double BYTES_PER_KB = 1024.0;
  private static final double BYTES_PER_MB = 1024.0 * 1024.0;
  private static final double BYTES_PER_GB = 1024.0 * 1024.0 * 1024.0;
  private static final double BYTES_PER_TB = 1024.0 * 1024.0 * 1024.0 * 1024.0;
  private static final double BYTES_PER_PB = 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0;
  
  // Constants for precision in tests
  private static final double SMALL_DELTA = 0.000_000_1;
  private static final double STANDARD_DELTA = 0.001;
  private static final double MEDIUM_DELTA = 0.01;

  /**
   * Tests small byte size values including zero and single byte.
   */
  @Test
  public void testSmallByteValues() {
    // Test zero and single byte values
    ByteSize singleByteSize = new ByteSize("1B");
    Assert.assertEquals(1L, singleByteSize.getBytes());
    Assert.assertEquals(0.000_976_563, singleByteSize.getKilobytes(), SMALL_DELTA);
    Assert.assertEquals("B", singleByteSize.getUnit());
    Assert.assertEquals(1.0, singleByteSize.getNumericValue(), STANDARD_DELTA);
    
    ByteSize zeroByteSize = new ByteSize("0B");
    Assert.assertEquals(0L, zeroByteSize.getBytes());
    Assert.assertEquals(0.0, zeroByteSize.getKilobytes(), STANDARD_DELTA);
    Assert.assertEquals("B", zeroByteSize.getUnit());
    Assert.assertEquals(0.0, zeroByteSize.getNumericValue(), STANDARD_DELTA);
  }

  /**
   * Tests basic byte values.
   */
  @Test
  public void testByteValues() {
    // Test bytes
    ByteSize byteSize = new ByteSize("1024B");
    Assert.assertEquals(1024, byteSize.getBytes());
    Assert.assertEquals(1.0, byteSize.getKilobytes(), STANDARD_DELTA);
    Assert.assertEquals(0.000_976_563, byteSize.getMegabytes(), SMALL_DELTA);
    Assert.assertEquals("B", byteSize.getUnit());
    Assert.assertEquals(1024.0, byteSize.getNumericValue(), STANDARD_DELTA);
    
    // Test kilobytes
    ByteSize kilobyteSize = new ByteSize("2KB");
    Assert.assertEquals(2 * BYTES_PER_KB, kilobyteSize.getBytes(), 0.1); // Use delta for double conversion
    Assert.assertEquals(2.0, kilobyteSize.getKilobytes(), STANDARD_DELTA);
    // Fix the precision issue - the expected value is exactly 0.001953125 (2/1024)
    double expectedGigabytes = 2.0 / (BYTES_PER_KB * BYTES_PER_KB);
    Assert.assertEquals(expectedGigabytes, kilobyteSize.getGigabytes(), SMALL_DELTA);
    Assert.assertEquals("KB", kilobyteSize.getUnit());
    Assert.assertEquals(2.0, kilobyteSize.getNumericValue(), STANDARD_DELTA);
    
    // Test megabytes with decimal point
    ByteSize megabyteSize = new ByteSize("1.5MB");
    Assert.assertEquals((long) (1.5 * BYTES_PER_MB), megabyteSize.getBytes(), 0.1); // Use delta for double conversion
    Assert.assertEquals(1.5, megabyteSize.getMegabytes(), STANDARD_DELTA);
    Assert.assertEquals("MB", megabyteSize.getUnit());
    Assert.assertEquals(1.5, megabyteSize.getNumericValue(), STANDARD_DELTA);
    
    // Test gigabytes
    ByteSize gigabyteSize = new ByteSize("2GB");
    Assert.assertEquals((long) (2 * BYTES_PER_GB), gigabyteSize.getBytes(), 0.1); // Use delta for double conversion
    Assert.assertEquals(2.0, gigabyteSize.getGigabytes(), STANDARD_DELTA);
    Assert.assertEquals("GB", gigabyteSize.getUnit());
    
    // Test terabytes
    ByteSize terabyteSize = new ByteSize("1TB");
    Assert.assertEquals((long) BYTES_PER_TB, terabyteSize.getBytes(), 0.1); // Use delta for double conversion
    Assert.assertEquals("TB", terabyteSize.getUnit());
    
    // Test petabytes
    ByteSize petabyteSize = new ByteSize("0.1PB");
    Assert.assertEquals((long) (0.1 * BYTES_PER_PB), petabyteSize.getBytes(), 0.1); // Use delta for double conversion
    Assert.assertEquals("PB", petabyteSize.getUnit());
  }

  /**
   * Tests unit conversions between different byte size units.
   */
  @Test
  public void testByteConversions() {
    // Test 1 MB in various units
    ByteSize oneMegabyteSize = new ByteSize("1MB");
    Assert.assertEquals(BYTES_PER_MB, oneMegabyteSize.getBytes(), 0);
    Assert.assertEquals(BYTES_PER_KB, oneMegabyteSize.getKilobytes(), STANDARD_DELTA);
    Assert.assertEquals(1.0, oneMegabyteSize.getMegabytes(), STANDARD_DELTA);
    Assert.assertEquals(1.0 / BYTES_PER_KB, oneMegabyteSize.getGigabytes(), SMALL_DELTA);
    
    // Test 1 GB in various units
    ByteSize oneGigabyteSize = new ByteSize("1GB");
    Assert.assertEquals(BYTES_PER_GB, oneGigabyteSize.getBytes(), 0);
    Assert.assertEquals(BYTES_PER_GB / BYTES_PER_KB, oneGigabyteSize.getKilobytes(), STANDARD_DELTA);
    Assert.assertEquals(BYTES_PER_KB, oneGigabyteSize.getMegabytes(), STANDARD_DELTA);
    Assert.assertEquals(1.0, oneGigabyteSize.getGigabytes(), STANDARD_DELTA);
    Assert.assertEquals(1.0 / BYTES_PER_KB, oneGigabyteSize.getTerabytes(), SMALL_DELTA);
    
    // Test 1 TB in various units
    ByteSize oneTerabyteSize = new ByteSize("1TB");
    Assert.assertEquals(BYTES_PER_TB, oneTerabyteSize.getBytes(), 0);
    Assert.assertEquals(BYTES_PER_TB / BYTES_PER_KB, oneTerabyteSize.getKilobytes(), STANDARD_DELTA);
    Assert.assertEquals(BYTES_PER_TB / BYTES_PER_MB, oneTerabyteSize.getMegabytes(), STANDARD_DELTA);
    Assert.assertEquals(BYTES_PER_KB, oneTerabyteSize.getGigabytes(), STANDARD_DELTA);
    Assert.assertEquals(1.0, oneTerabyteSize.getTerabytes(), STANDARD_DELTA);
    Assert.assertEquals(1.0 / BYTES_PER_KB, oneTerabyteSize.getPetabytes(), SMALL_DELTA);
  }

  /**
   * Tests fractional byte size values.
   */
  @Test
  public void testFractionalByteValues() {
    // Test different units with fractional values
    ByteSize fractionalKilobyteSize = new ByteSize("0.5KB");
    Assert.assertEquals((long) (0.5 * BYTES_PER_KB), fractionalKilobyteSize.getBytes());
    Assert.assertEquals(0.5, fractionalKilobyteSize.getKilobytes(), STANDARD_DELTA);
    
    ByteSize fractionalMegabyteSize = new ByteSize("0.25MB");
    Assert.assertEquals((long) (0.25 * BYTES_PER_MB), fractionalMegabyteSize.getBytes());
    Assert.assertEquals(0.25, fractionalMegabyteSize.getMegabytes(), STANDARD_DELTA);
    
    ByteSize fractionalGigabyteSize = new ByteSize("0.125GB");
    Assert.assertEquals((long) (0.125 * BYTES_PER_GB), fractionalGigabyteSize.getBytes());
    Assert.assertEquals(0.125, fractionalGigabyteSize.getGigabytes(), STANDARD_DELTA);
    
    ByteSize fractionalTerabyteSize = new ByteSize("0.0625TB");
    Assert.assertEquals((long) (0.0625 * BYTES_PER_TB), fractionalTerabyteSize.getBytes());
    Assert.assertEquals(0.0625, fractionalTerabyteSize.getTerabytes(), STANDARD_DELTA);
    
    ByteSize fractionalPetabyteSize = new ByteSize("0.03125PB");
    Assert.assertEquals((long) (0.03125 * BYTES_PER_PB), fractionalPetabyteSize.getBytes());
    Assert.assertEquals(0.03125, fractionalPetabyteSize.getPetabytes(), STANDARD_DELTA);
  }

  /**
   * Tests large byte size values.
   */
  @Test
  public void testLargeByteValues() {
    // Test large values in various units
    ByteSize largeKilobyteSize = new ByteSize("999999KB");
    Assert.assertEquals((long) (999_999 * BYTES_PER_KB), largeKilobyteSize.getBytes());
    Assert.assertEquals(999_999.0, largeKilobyteSize.getKilobytes(), STANDARD_DELTA);
    Assert.assertEquals(976.56, largeKilobyteSize.getMegabytes(), MEDIUM_DELTA);
    
    ByteSize largeMegabyteSize = new ByteSize("8192MB");
    Assert.assertEquals((long) (8192 * BYTES_PER_MB), largeMegabyteSize.getBytes());
    Assert.assertEquals(8.0, largeMegabyteSize.getGigabytes(), STANDARD_DELTA);
    
    ByteSize largeGigabyteSize = new ByteSize("4096GB");
    Assert.assertEquals((long) (4096 * BYTES_PER_GB), largeGigabyteSize.getBytes());
    Assert.assertEquals(4.0, largeGigabyteSize.getTerabytes(), STANDARD_DELTA);
  }

  /**
   * Tests validation for extreme values including very small and very large byte sizes.
   */
  @Test
  public void testExtremeValues() {
    // Test extremely small fractional values (should round to 1 byte)
    ByteSize verySmallByteSize = new ByteSize("0.000001MB");
    Assert.assertEquals(1, verySmallByteSize.getBytes());
    
    // Test extremely large values
    ByteSize veryLargeByteSize = new ByteSize("9999PB");
    Assert.assertTrue(veryLargeByteSize.getBytes() > 0);
    
    // Test values exactly at power boundary
    ByteSize exactPowerByteSize = new ByteSize("1024MB");
    Assert.assertEquals(1.0, exactPowerByteSize.getGigabytes(), SMALL_DELTA);
  }

  /**
   * Tests case insensitivity for byte size units.
   */
  @Test
  public void testCaseInsensitivity() {
    // Test all units with different cases
    ByteSize lowerCaseByteSize = new ByteSize("10b");
    ByteSize upperCaseByteSize = new ByteSize("10B");
    Assert.assertEquals(lowerCaseByteSize.getBytes(), upperCaseByteSize.getBytes());
    
    ByteSize lowerCaseKilobyteSize = new ByteSize("10kb");
    ByteSize upperCaseKilobyteSize = new ByteSize("10KB");
    Assert.assertEquals(lowerCaseKilobyteSize.getBytes(), upperCaseKilobyteSize.getBytes());
    
    ByteSize lowerCaseMegabyteSize = new ByteSize("10mb");
    ByteSize upperCaseMegabyteSize = new ByteSize("10MB");
    Assert.assertEquals(lowerCaseMegabyteSize.getBytes(), upperCaseMegabyteSize.getBytes());
    
    ByteSize lowerCaseGigabyteSize = new ByteSize("10gb");
    ByteSize upperCaseGigabyteSize = new ByteSize("10GB");
    Assert.assertEquals(lowerCaseGigabyteSize.getBytes(), upperCaseGigabyteSize.getBytes());
    
    ByteSize lowerCaseTerabyteSize = new ByteSize("10tb");
    ByteSize upperCaseTerabyteSize = new ByteSize("10TB");
    Assert.assertEquals(lowerCaseTerabyteSize.getBytes(), upperCaseTerabyteSize.getBytes());
    
    ByteSize lowerCasePetabyteSize = new ByteSize("10pb");
    ByteSize upperCasePetabyteSize = new ByteSize("10PB");
    Assert.assertEquals(lowerCasePetabyteSize.getBytes(), upperCasePetabyteSize.getBytes());
  }
  
  /**
   * Tests how the ByteSize class handles whitespace.
   */
  @Test
  public void testWhitespaceHandling() {
    // Test whitespace between value and unit
    ByteSize withoutSpaceSize = new ByteSize("10KB");
    ByteSize withSpaceSize = new ByteSize("10 KB");
    Assert.assertEquals(withoutSpaceSize.getBytes(), withSpaceSize.getBytes());
    
    ByteSize withMultipleSpacesSize = new ByteSize("10   KB");
    Assert.assertEquals(withoutSpaceSize.getBytes(), withMultipleSpacesSize.getBytes());
  }
  
  /**
   * Tests the toString method returns the original string representation.
   */
  @Test
  public void testToString() {
    // Original string should be preserved in toString()
    String byteSizeString = "1024KB";
    ByteSize byteSizeObject = new ByteSize(byteSizeString);
    Assert.assertEquals(byteSizeString, byteSizeObject.toString());
    
    String megabyteSizeString = "2.5MB";
    ByteSize megabyteSizeObject = new ByteSize(megabyteSizeString);
    Assert.assertEquals(megabyteSizeString, megabyteSizeObject.toString());
  }

  /**
   * Tests that ByteSize implements the Token interface correctly.
   */
  @Test
  public void testTokenInterface() {
    // Test Token interface methods and JSON representation
    ByteSize byteSizeObject = new ByteSize("10MB");
    Assert.assertEquals("10MB", byteSizeObject.value());
    Assert.assertEquals(TokenType.BYTE_SIZE, byteSizeObject.type());
    
    JsonElement jsonElement = byteSizeObject.toJson();
    Assert.assertTrue(jsonElement instanceof JsonObject);
    JsonObject jsonObject = (JsonObject) jsonElement;
    
    Assert.assertEquals("BYTE_SIZE", jsonObject.get("type").getAsString());
    Assert.assertEquals("10MB", jsonObject.get("value").getAsString());
    Assert.assertEquals(10 * 1024 * 1024, jsonObject.get("bytes").getAsLong());
  }

  // ---------- Exception Tests ----------
  
  /**
   * Tests that invalid format throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new ByteSize("10XB"); // Invalid unit should throw exception
  }
  
  /**
   * Tests that non-numeric value throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNumber() {
    new ByteSize("ABCKB"); // Not a number should throw exception
  }
  
  /**
   * Tests that missing unit throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testMissingUnit() {
    new ByteSize("10"); // Missing unit should throw exception
  }
  
  /**
   * Tests that incorrect unit format throws appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIncorrectUnitFormat() {
    new ByteSize("10K"); // Incomplete unit format should throw exception
  }
  
  /**
   * Tests that negative values throw appropriate exception.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNegativeValue() {
    new ByteSize("-10KB"); // Negative values should throw exception
  }
}
