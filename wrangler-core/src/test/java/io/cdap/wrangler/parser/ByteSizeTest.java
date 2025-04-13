/*
 * Copyright © 2023-2025 Cask Data, Inc.
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
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

/**
 * Tests for {@link ByteSize} class
 */
public class ByteSizeTest {

  @Test
  public void testKilobytes() throws Exception {
    ByteSize size = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024L, size.getBytes());
    Assert.assertEquals(10.0, size.getKilobytes(), 0.001);
    Assert.assertEquals("10KB", size.value());
    Assert.assertEquals(TokenType.BYTE_SIZE, size.type());
  }

  @Test
  public void testMegabytes() throws Exception {
    ByteSize size = new ByteSize("1.5MB");
    Assert.assertEquals(1.5 * 1024 * 1024, size.getBytes(), 1.0); // Allow for small rounding differences
    Assert.assertEquals(1.5, size.getMegabytes(), 0.001);
  }

  @Test
  public void testGigabytes() throws Exception {
    ByteSize size = new ByteSize("2GB");
    Assert.assertEquals(2L * 1024 * 1024 * 1024, size.getBytes());
    Assert.assertEquals(2.0, size.getGigabytes(), 0.001);
  }

  @Test
  public void testTerabytes() throws Exception {
    ByteSize size = new ByteSize("1TB");
    Assert.assertEquals(1024L * 1024 * 1024 * 1024, size.getBytes());
    Assert.assertEquals(1.0, size.getTerabytes(), 0.001);
  }

  @Test
  public void testCaseInsensitiveUnits() throws Exception {
    ByteSize kb = new ByteSize("5kb");
    ByteSize KB = new ByteSize("5KB");
    ByteSize mb = new ByteSize("2mb");
    ByteSize MB = new ByteSize("2MB");
    
    Assert.assertEquals(kb.getBytes(), KB.getBytes());
    Assert.assertEquals(mb.getBytes(), MB.getBytes());
  }

  @Test(expected = ParseException.class)
  public void testInvalidFormat() throws Exception {
    new ByteSize("10megabytes");
  }

  @Test(expected = ParseException.class)
  public void testInvalidNumber() throws Exception {
    new ByteSize("KB");
  }

  @Test(expected = ParseException.class)
  public void testNegativeSize() throws Exception {
    new ByteSize("-10KB");
  }
}