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

/**
 * Unit tests for {@link ByteSize}.
 */
public class ByteSizeTest {

  @Test
  public void testValidByteSizes() {
    Assert.assertEquals(1024, new ByteSize("1KB").getBytes(), 0.001);
    Assert.assertEquals(2048, new ByteSize("2kb").getBytes(), 0.001); // case-insensitive
    Assert.assertEquals(1048576, new ByteSize("1MB").getBytes(), 0.001);
    Assert.assertEquals(1073741824L, new ByteSize("1GB").getBytes(), 0.001);
    Assert.assertEquals(1099511627776L, new ByteSize("1TB").getBytes(), 0.001);
  }

  @Test
  public void testDecimalByteSizes() {
    Assert.assertEquals(1536, new ByteSize("1.5KB").getBytes(), 0.001);
    Assert.assertEquals(1572864, new ByteSize("1.5MB").getBytes(), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidUnit() {
    new ByteSize("50XY");
  }

  @Test(expected = NumberFormatException.class)
  public void testMalformedNumber() {
    new ByteSize("abcMB");
  }

  @Test
  public void testWhitespaceHandling() {
    Assert.assertEquals(1024, new ByteSize(" 1KB ").getBytes(), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
    public void testNegativeValue() {
      new ByteSize("-1KB");
  }
}
