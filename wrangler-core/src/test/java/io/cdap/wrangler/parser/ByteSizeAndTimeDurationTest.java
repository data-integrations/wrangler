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
import io.cdap.wrangler.api.parser.TimeDuration;
import org.junit.Assert;
import org.junit.Test;

public class ByteSizeAndTimeDurationTest {

  @Test
  public void testByteSizeParsing() {
    ByteSize b1 = new ByteSize("10KB");
    ByteSize b2 = new ByteSize("1.5MB");
    ByteSize b3 = new ByteSize("2GB");

    Assert.assertEquals(10 * 1024L, b1.getBytes());
    Assert.assertEquals((long) (1.5 * 1024 * 1024), b2.getBytes());
    Assert.assertEquals(2L * 1024 * 1024 * 1024, b3.getBytes());
  }

  @Test
  public void testTimeDurationParsing() {
    TimeDuration t1 = new TimeDuration("150ms");
    TimeDuration t2 = new TimeDuration("2s");
    TimeDuration t3 = new TimeDuration("1.5m");

    Assert.assertEquals(150, t1.getMilliseconds());
    Assert.assertEquals(2000, t2.getMilliseconds());
    Assert.assertEquals((long) (1.5 * 60 * 1000), t3.getMilliseconds());
  }
}
