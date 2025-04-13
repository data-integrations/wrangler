/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
import org.junit.Assert;
import org.junit.Test;
import io.cdap.wrangler.api.parser.ByteSize;

public class ByteSizeTest {

  @Test
  public void testByteSizeParsing() {
    ByteSize byteSize1 = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024, (long) byteSize1.value());

    ByteSize byteSize2 = new ByteSize("1.5MB");
    Assert.assertEquals(1.5 * 1024 * 1024, (double) byteSize2.value(), 0.001);

    ByteSize byteSize3 = new ByteSize("10kb");
    Assert.assertEquals(10 * 1024, (long) byteSize3.value());

    ByteSize byteSize4 = new ByteSize("2.5GB");
    Assert.assertEquals(2.5 * 1024 * 1024 * 1024, (double) byteSize4.value(), 0.001);

    ByteSize byteSize5 = new ByteSize("1TB");
    Assert.assertEquals(1024L * 1024 * 1024 * 1024, (long) byteSize5.value());
  }

  @Test
  public void testByteSizeConversion() {
    long bytes = 1048576; // 1 MB

    double inMB = ByteSize.convert(bytes, "MB");
    Assert.assertEquals(1.0, inMB, 0.001);

    double inGB = ByteSize.convert(bytes, "GB");
    Assert.assertEquals(1.0 / 1024, inGB, 0.000001);

    double inKB = ByteSize.convert(bytes, "KB");
    Assert.assertEquals(1024.0, inKB, 0.001);

    double inBytes = ByteSize.convert(bytes, "B");
    Assert.assertEquals(1048576.0, inBytes, 0.001);
  }

  @Test
  public void testInvalidByteSize() {
    expectIllegalArgument(() -> new ByteSize("10ZZ"));
    expectIllegalArgument(() -> new ByteSize("-10KB"));
    expectIllegalArgument(() -> new ByteSize(""));
    expectIllegalArgument(() -> new ByteSize(null));
  }

  @Test
  public void testZeroByteSize() {
    ByteSize byteSize = new ByteSize("0KB");
    Assert.assertEquals(0L, (long) byteSize.value());
  }

  @Test
  public void testCanonicalValueRetrieval() {
    ByteSize byteSize1 = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024, (long) byteSize1.value());

    ByteSize byteSize2 = new ByteSize("10kb");
    Assert.assertEquals(10 * 1024, (long) byteSize2.value());
  }

  private void expectIllegalArgument(Runnable runnable) {
    try {
      runnable.run();
      Assert.fail("Expected IllegalArgumentException.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
