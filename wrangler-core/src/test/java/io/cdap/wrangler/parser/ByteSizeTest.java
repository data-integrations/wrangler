/*
 * Copyright © 2017-2022 Cask Data, Inc.
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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.ByteSize.ByteUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize} class.
 */
public class ByteSizeTest {

  @Test
  public void testByteSizeParsing() {
    // Test parsing various byte size formats
    ByteSize b1 = new ByteSize("10B");
    Assert.assertEquals(10L, b1.getBytes());
    Assert.assertEquals(ByteUnit.B, b1.getUnit());
    Assert.assertEquals(10.0, b1.getValue(), 0.001);

    ByteSize b2 = new ByteSize("1.5KB");
    Assert.assertEquals(1536L, b2.getBytes());
    Assert.assertEquals(ByteUnit.KB, b2.getUnit());
    Assert.assertEquals(1.5, b2.getValue(), 0.001);

    ByteSize b3 = new ByteSize("2MB");
    Assert.assertEquals(2 * 1024 * 1024L, b3.getBytes());
    Assert.assertEquals(ByteUnit.MB, b3.getUnit());

    ByteSize b4 = new ByteSize("3.25GB");
    Assert.assertEquals(3.25 * 1024 * 1024 * 1024L, b4.getBytes(), 1024); // Allow some floating point imprecision
    Assert.assertEquals(ByteUnit.GB, b4.getUnit());
  }

  @Test
  public void testCaseInsensitivity() {
    // Test case insensitivity in unit parsing
    ByteSize b1 = new ByteSize("10b");
    Assert.assertEquals(10L, b1.getBytes());
    Assert.assertEquals(ByteUnit.B, b1.getUnit());

    ByteSize b2 = new ByteSize("1.5Kb");
    Assert.assertEquals(1536L, b2.getBytes());
    Assert.assertEquals(ByteUnit.KB, b2.getUnit());

    ByteSize b3 = new ByteSize("2mb");
    Assert.assertEquals(2 * 1024 * 1024L, b3.getBytes());
    Assert.assertEquals(ByteUnit.MB, b3.getUnit());
  }

  @Test
  public void testConversions() {
    // Test conversion between units
    ByteSize original = new ByteSize("5MB");
    
    // Convert to KB
    Assert.assertEquals(5 * 1024, original.convertTo(ByteUnit.KB), 0.001);
    
    // Convert to GB
    Assert.assertEquals(5.0 / 1024, original.convertTo(ByteUnit.GB), 0.001);
    
    // Convert to same unit (should be unchanged)
    Assert.assertEquals(5.0, original.convertTo(ByteUnit.MB), 0.001);
    
    // Test to() method
    ByteSize inKB = original.to(ByteUnit.KB);
    Assert.assertEquals(ByteUnit.KB, inKB.getUnit());
    Assert.assertEquals(5 * 1024, inKB.getValue(), 0.001);
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    // Test invalid format (missing unit)
    new ByteSize("100");
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    // Test invalid unit
    new ByteSize("100XB");
  }
  
  @Test
  public void testFromBytes() {
    // Test creating ByteSize from bytes
    ByteSize bs = ByteSize.fromBytes(1024 * 1024);
    Assert.assertEquals(ByteUnit.B, bs.getUnit());
    Assert.assertEquals(1024 * 1024, bs.getValue(), 0.001);
    
    // Convert to MB for a more readable form
    ByteSize inMB = bs.to(ByteUnit.MB);
    Assert.assertEquals(ByteUnit.MB, inMB.getUnit());
    Assert.assertEquals(1.0, inMB.getValue(), 0.001);
  }
  
  @Test
  public void testParseNullable() {
    // Test null handling
    Assert.assertNull(ByteSize.parse(null));
    Assert.assertNull(ByteSize.parse(""));
    Assert.assertNull(ByteSize.parse("  "));
    
    // Test successful parsing
    ByteSize bs = ByteSize.parse("10MB");
    Assert.assertNotNull(bs);
    Assert.assertEquals(10 * 1024 * 1024L, bs.getBytes());
  }
  
  @Test
  public void testFormatting() {
    // Test formatting
    ByteSize bs = new ByteSize("10.5MB");
    Assert.assertEquals("10.5MB", bs.format(1));
    Assert.assertEquals("10.50MB", bs.format(2));
  }
} 
