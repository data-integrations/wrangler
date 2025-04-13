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
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.parser;

import org.junit.Assert;
import org.junit.Test;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TokenType;

/**
 * Tests for {@link ByteSize} token class.
 */
public class ByteSizeTest {

  @Test
  public void testByteSizeTokenType() {
    ByteSize byteSize = new ByteSize("10MB");
    Assert.assertEquals(TokenType.BYTE_SIZE, byteSize.type());
  }

  @Test
  public void testByteSizeParsing() {
    ByteSize kb = new ByteSize("1KB");
    ByteSize mb = new ByteSize("1MB");
    ByteSize gb = new ByteSize("1GB");
    ByteSize tb = new ByteSize("1TB");
    ByteSize pb = new ByteSize("1PB");

    Assert.assertEquals(1024L, kb.getBytes());
    Assert.assertEquals(1024L * 1024L, mb.getBytes());
    Assert.assertEquals(1024L * 1024L * 1024L, gb.getBytes());
    Assert.assertEquals(1024L * 1024L * 1024L * 1024L, tb.getBytes());
    Assert.assertEquals(1024L * 1024L * 1024L * 1024L * 1024L, pb.getBytes());
  }

  @Test
  public void testByteSizeDecimalParsing() {
    ByteSize mb = new ByteSize("1.5MB");
    Assert.assertEquals((long) (1.5 * 1024L * 1024L), mb.getBytes());
  }

  @Test
  public void testByteSizeConversions() {
    ByteSize mb = new ByteSize("1MB");
    Assert.assertEquals(1.0, mb.getMegabytes(), 0.0001);
    Assert.assertEquals(1024.0, mb.getKilobytes(), 0.0001);
    Assert.assertEquals(1024.0 * 1024.0, mb.getBytes(), 0.0001);
    Assert.assertEquals(1.0 / 1024.0, mb.getGigabytes(), 0.0001);
  }

  @Test
  public void testValueMethod() {
    ByteSize byteSize = new ByteSize("10MB");
    Assert.assertEquals("10MB", byteSize.value());
  }

  @Test
  public void testToStringMethod() {
    ByteSize byteSize = new ByteSize("10MB");
    Assert.assertEquals("10MB", byteSize.toString());
  }

  @Test
  public void testCaseInsensitivity() {
    ByteSize mb1 = new ByteSize("1MB");
    ByteSize mb2 = new ByteSize("1mb");
    Assert.assertEquals(mb1.getBytes(), mb2.getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSizeFormat() {
    new ByteSize("invalid");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSizeUnit() {
    new ByteSize("10XB");
  }
}