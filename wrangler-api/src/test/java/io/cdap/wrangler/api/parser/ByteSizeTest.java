/*
 * Copyright © 2024 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

  @Test
  public void testValidByteSizes() {
    // Test bytes
    ByteSize bytes = new ByteSize("1024B");
    Assert.assertEquals(1024L, bytes.getBytes());
    Assert.assertEquals(1.0, bytes.getKilobytes(), 0.001);

    // Test kilobytes
    ByteSize kb = new ByteSize("1.5KB");
    Assert.assertEquals(1536L, kb.getBytes());
    Assert.assertEquals(1.5, kb.getKilobytes(), 0.001);

    // Test megabytes
    ByteSize mb = new ByteSize("2.5MB");
    Assert.assertEquals(2621440L, mb.getBytes());
    Assert.assertEquals(2560.0, mb.getKilobytes(), 0.001);
    Assert.assertEquals(2.5, mb.getMegabytes(), 0.001);

    // Test gigabytes
    ByteSize gb = new ByteSize("1.5GB");
    Assert.assertEquals(1610612736L, gb.getBytes());
    Assert.assertEquals(1536.0, gb.getMegabytes(), 0.001);
    Assert.assertEquals(1.5, gb.getGigabytes(), 0.001);

    // Test terabytes
    ByteSize tb = new ByteSize("2TB");
    Assert.assertEquals(2199023255552L, tb.getBytes());
    Assert.assertEquals(2048.0, tb.getGigabytes(), 0.001);

    // Test petabytes
    ByteSize pb = new ByteSize("1PB");
    Assert.assertEquals(1125899906842624L, pb.getBytes());
  }

  @Test
  public void testCaseInsensitivity() {
    ByteSize kb1 = new ByteSize("1kb");
    ByteSize kb2 = new ByteSize("1KB");
    ByteSize kb3 = new ByteSize("1Kb");
    
    Assert.assertEquals(kb1.getBytes(), kb2.getBytes());
    Assert.assertEquals(kb1.getBytes(), kb3.getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat() {
    new ByteSize("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new ByteSize("1ZB");
  }

  @Test
  public void testToString() {
    ByteSize size = new ByteSize("1.5MB");
    Assert.assertEquals("1.5MB", size.toString());
  }

  @Test
  public void testToJson() {
    ByteSize size = new ByteSize("1.5MB");
    Assert.assertEquals("BYTE_SIZE", size.toJson().getAsJsonObject().get("type").getAsString());
    Assert.assertEquals("1.5MB", size.toJson().getAsJsonObject().get("value").getAsString());
    Assert.assertEquals(1572864L, size.toJson().getAsJsonObject().get("bytes").getAsLong());
  }
} 