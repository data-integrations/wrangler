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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTest {

  @Test
  public void testByteParsing() {
    ByteSize size = new ByteSize("100B");
    Assert.assertEquals(100, size.getBytes());
    Assert.assertEquals(0.09765625, size.getKilobytes(), 0.00001);
  }

  @Test
  public void testKilobyteParsing() {
    ByteSize size = new ByteSize("10KB");
    Assert.assertEquals(10240, size.getBytes());
    Assert.assertEquals(10, size.getKilobytes(), 0.00001);
    Assert.assertEquals(0.01, size.getMegabytes(), 0.00001);
  }

  @Test
  public void testMegabyteParsing() {
    ByteSize size = new ByteSize("1.5MB");
    Assert.assertEquals(1572864, size.getBytes());
    Assert.assertEquals(1.5, size.getMegabytes(), 0.00001);
  }

  @Test
  public void testGigabyteParsing() {
    ByteSize size = new ByteSize("2GB");
    Assert.assertEquals(2147483648L, size.getBytes());
    Assert.assertEquals(2, size.getGigabytes(), 0.00001);
  }

  @Test
  public void testTerabyteParsing() {
    ByteSize size = new ByteSize("0.1TB");
    Assert.assertEquals(109951162777L, size.getBytes());
    Assert.assertEquals(0.1, size.getTerabytes(), 0.00001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new ByteSize("10XB");
  }

  @Test(expected = NumberFormatException.class)
  public void testInvalidNumber() {
    new ByteSize("ABC");
  }
}