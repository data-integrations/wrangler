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

package io.cdap.wrangler.api.parser;

import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

  @Test
  public void testValidByteSizes() {
    ByteSize b = new ByteSize("100B");
    Assert.assertEquals(100L, b.getBytes());

    ByteSize kb = new ByteSize("2KB");
    Assert.assertEquals(2048L, kb.getBytes());

    ByteSize mb = new ByteSize("1.5MB");
    Assert.assertEquals(1572864L, mb.getBytes());

    ByteSize gb = new ByteSize("1GB");
    Assert.assertEquals(1073741824L, gb.getBytes());

    ByteSize tb = new ByteSize("1TB");
    Assert.assertEquals(1099511627776L, tb.getBytes());
  }

  @Test
  public void testLowerCaseUnit() {
    ByteSize b = new ByteSize("3kb");
    Assert.assertEquals(3072L, b.getBytes());  // should handle lowercase correctly
  }

  @Test
  public void testTrimmedInput() {
    ByteSize b = new ByteSize("  4 MB ");
    Assert.assertEquals(4194304L, b.getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat_noUnit() {
    new ByteSize("1234");  // No unit, should fail
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat_invalidUnit() {
    new ByteSize("1PB");  // Unsupported unit
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidFormat_invalidNumber() {
    new ByteSize("abcMB");  // Invalid numeric value
  }

  @Test
  public void testToJsonOutput() {
    ByteSize kb = new ByteSize("10KB");
    String json = kb.toJson().toString();
    Assert.assertTrue(json.contains("\"token\":\"10KB\""));
    Assert.assertTrue(json.contains("\"value\":10.0"));
    Assert.assertTrue(json.contains("\"unit\":\"KB\""));
    Assert.assertTrue(json.contains("\"bytes\":10240"));
  }

  @Test
  public void testType() {
    ByteSize mb = new ByteSize("1MB");
    Assert.assertEquals(TokenType.BYTE_SIZE, mb.type());
  }
}
