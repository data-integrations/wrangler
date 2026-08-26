/*
 * Copyright © 2023 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize} class.
 */
public class ByteSizeTest {

  @Test
  public void testByteSizeConstruction() {
    ByteSize b1 = new ByteSize("10B");
    Assert.assertEquals(10L, b1.getBytes());
    
    ByteSize b2 = new ByteSize("1.5KB");
    Assert.assertEquals(1536L, b2.getBytes());
    
    ByteSize b3 = new ByteSize("2MB");
    Assert.assertEquals(2 * 1024 * 1024L, b3.getBytes());
    
    ByteSize b4 = new ByteSize("3.5GB");
    Assert.assertEquals((long) (3.5 * 1024 * 1024 * 1024), b4.getBytes());
    
    ByteSize b5 = new ByteSize("1TB");
    Assert.assertEquals(1024L * 1024 * 1024 * 1024, b5.getBytes());
    
    ByteSize b6 = new ByteSize("0.1PB");
    Assert.assertEquals((long) (0.1 * 1024 * 1024 * 1024 * 1024 * 1024), b6.getBytes());
  }

  @Test
  public void testByteSizeGetters() {
    ByteSize b = new ByteSize("1024KB");
    
    Assert.assertEquals(1024.0, b.getValue(), 0.001);
    Assert.assertEquals("KB", b.getUnit());
    Assert.assertEquals(1048576L, b.getBytes());
    Assert.assertEquals(1024.0, b.getKilobytes(), 0.001);
    Assert.assertEquals(1.0, b.getMegabytes(), 0.001);
    Assert.assertEquals(0.0009765625, b.getGigabytes(), 0.0000001);
  }

  @Test
  public void testByteSizeConversion() {
    ByteSize b = new ByteSize("2.5MB");
    
    Assert.assertEquals(2.5 * 1024 * 1024, b.convertTo("B"), 0.001);
    Assert.assertEquals(2.5 * 1024, b.convertTo("KB"), 0.001);
    Assert.assertEquals(2.5, b.convertTo("MB"), 0.001);
    Assert.assertEquals(2.5 / 1024, b.convertTo("GB"), 0.0000001);
    Assert.assertEquals(2.5 / (1024 * 1024), b.convertTo("TB"), 0.0000001);
    Assert.assertEquals(2.5 / (1024 * 1024 * 1024), b.convertTo("PB"), 0.0000001);
  }

  @Test
  public void testEqualsAndHashCode() {
    ByteSize b1 = new ByteSize("1MB");
    ByteSize b2 = new ByteSize("1024KB");
    ByteSize b3 = new ByteSize("2MB");
    
    Assert.assertEquals(b1, b2);
    Assert.assertEquals(b1.hashCode(), b2.hashCode());
    
    Assert.assertNotEquals(b1, b3);
    Assert.assertNotEquals(b1.hashCode(), b3.hashCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSize() {
    new ByteSize("10XB");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSizeFormat() {
    new ByteSize("KB");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSizeConversion() {
    ByteSize b = new ByteSize("10MB");
    b.convertTo("XB");
  }
}
