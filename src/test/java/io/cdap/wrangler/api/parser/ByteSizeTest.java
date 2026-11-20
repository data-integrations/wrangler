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

package io.cdap.wrangler.api.parser;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize} class.
 */
public class ByteSizeTest {

  @Test
  public void testParsing() {
    // Test various valid byte size strings
    ByteSize bs1 = new ByteSize("10B");
    Assert.assertEquals(10, bs1.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.B, bs1.getUnit());
    Assert.assertEquals(10, bs1.getValue(), 0.001);

    ByteSize bs2 = new ByteSize("5KB");
    Assert.assertEquals(5 * 1024, bs2.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.KB, bs2.getUnit());
    Assert.assertEquals(5, bs2.getValue(), 0.001);

    ByteSize bs3 = new ByteSize("2.5MB");
    Assert.assertEquals((long) (2.5 * 1024 * 1024), bs3.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.MB, bs3.getUnit());
    Assert.assertEquals(2.5, bs3.getValue(), 0.001);

    ByteSize bs4 = new ByteSize("1.75GB");
    Assert.assertEquals((long) (1.75 * 1024 * 1024 * 1024), bs4.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.GB, bs4.getUnit());
    Assert.assertEquals(1.75, bs4.getValue(), 0.001);

    // Case insensitivity
    ByteSize bs5 = new ByteSize("3mb");
    Assert.assertEquals(3 * 1024 * 1024, bs5.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.MB, bs5.getUnit());

    // Allowed whitespace
    ByteSize bs6 = new ByteSize("4 KB");
    Assert.assertEquals(4 * 1024, bs6.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.KB, bs6.getUnit());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    // Missing unit
    new ByteSize("10");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    // Invalid unit
    new ByteSize("10XB");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonNumericValue() {
    // Non-numeric value
    new ByteSize("abc KB");
  }

  @Test
  public void testConversion() {
    ByteSize bs = new ByteSize("1024KB");
    
    // Convert to different units
    Assert.assertEquals(1024 * 1024, bs.getBytes());
    Assert.assertEquals(1, bs.convertTo(ByteSize.ByteUnit.MB), 0.001);
    Assert.assertEquals(1024, bs.convertTo(ByteSize.ByteUnit.KB), 0.001);
    Assert.assertEquals(0.001, bs.convertTo(ByteSize.ByteUnit.GB), 0.001);
    
    // Test to() method
    ByteSize mb = bs.to(ByteSize.ByteUnit.MB);
    Assert.assertEquals(ByteSize.ByteUnit.MB, mb.getUnit());
    Assert.assertEquals(1, mb.getValue(), 0.001);
    Assert.assertEquals(1024 * 1024, mb.getBytes());
    
    // Test conversion to larger unit
    ByteSize gb = new ByteSize("2048MB").to(ByteSize.ByteUnit.GB);
    Assert.assertEquals(ByteSize.ByteUnit.GB, gb.getUnit());
    Assert.assertEquals(2, gb.getValue(), 0.001);
  }

  @Test
  public void testStaticMethods() {
    // Test fromBytes()
    ByteSize bs = ByteSize.fromBytes(1024);
    Assert.assertEquals(1024, bs.getBytes());
    Assert.assertEquals(ByteSize.ByteUnit.B, bs.getUnit());
    
    // Test parse() with null/empty
    Assert.assertNull(ByteSize.parse(null));
    Assert.assertNull(ByteSize.parse(""));
    Assert.assertNull(ByteSize.parse("  "));
    
    // Test parse() with valid value
    ByteSize parsed = ByteSize.parse("5MB");
    Assert.assertEquals(5 * 1024 * 1024, parsed.getBytes());
  }

  @Test
  public void testFormat() {
    ByteSize bs = new ByteSize(2.5, ByteSize.ByteUnit.MB);
    
    // Test format with different precisions
    Assert.assertEquals("2.5MB", bs.format(1));
    Assert.assertEquals("2.50MB", bs.format(2));
    Assert.assertEquals("2.500MB", bs.format(3));
  }

  @Test
  public void testTokenInterface() {
    ByteSize bs = new ByteSize("1.5GB");
    
    // Test Token interface methods
    Assert.assertEquals(TokenType.BYTE_SIZE, bs.type());
    Assert.assertEquals(bs, bs.value());
    Assert.assertEquals("1.5GB", bs.toString());
    
    // Test toJson
    JsonObject json = bs.toJson().getAsJsonObject();
    Assert.assertEquals("BYTE_SIZE", json.get("type").getAsString());
    Assert.assertEquals("1.5GB", json.get("value").getAsString());
    Assert.assertEquals("GB", json.get("unit").getAsString());
    Assert.assertEquals((long) (1.5 * 1024 * 1024 * 1024), json.get("bytes").getAsLong());
  }
} 
