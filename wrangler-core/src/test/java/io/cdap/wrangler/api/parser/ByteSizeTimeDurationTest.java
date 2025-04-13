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
 * Tests for ByteSize and TimeDuration token parsing and conversion.
 */
public class ByteSizeTimeDurationTest {

  @Test
  public void testByteSizeParsing() {
    // Test basic byte sizes
    Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
    Assert.assertEquals(1024 * 1024L, new ByteSize("1MB").getBytes());
    Assert.assertEquals(1024 * 1024 * 1024L, new ByteSize("1GB").getBytes());
    Assert.assertEquals(1024L * 1024 * 1024 * 1024, new ByteSize("1TB").getBytes());

    // Test decimal values
    Assert.assertEquals(1536L, new ByteSize("1.5KB").getBytes());
    Assert.assertEquals(1536 * 1024L, new ByteSize("1.5MB").getBytes());

    // Test case insensitivity
    Assert.assertEquals(1024L, new ByteSize("1kb").getBytes());
    Assert.assertEquals(1024 * 1024L, new ByteSize("1mb").getBytes());

    // Test invalid values
    Assert.assertEquals(0L, new ByteSize("invalid").getBytes());
    Assert.assertEquals(0L, new ByteSize("1.2.3KB").getBytes());
  }

  @Test
  public void testTimeDurationParsing() {
    // Test basic time durations
    Assert.assertEquals(1000L, new TimeDuration("1s").getMilliseconds());
    Assert.assertEquals(60 * 1000L, new TimeDuration("1m").getMilliseconds());
    Assert.assertEquals(60 * 60 * 1000L, new TimeDuration("1h").getMilliseconds());
    Assert.assertEquals(24 * 60 * 60 * 1000L, new TimeDuration("1d").getMilliseconds());

    // Test decimal values
    Assert.assertEquals(1500L, new TimeDuration("1.5s").getMilliseconds());
    Assert.assertEquals(90 * 1000L, new TimeDuration("1.5m").getMilliseconds());

    // Test case insensitivity
    Assert.assertEquals(1000L, new TimeDuration("1S").getMilliseconds());
    Assert.assertEquals(60 * 1000L, new TimeDuration("1M").getMilliseconds());

    // Test invalid values
    Assert.assertEquals(0L, new TimeDuration("invalid").getMilliseconds());
    Assert.assertEquals(0L, new TimeDuration("1.2.3s").getMilliseconds());
  }

  @Test
  public void testByteSizeConversion() {
    ByteSize size = new ByteSize("1.5MB");
    Assert.assertEquals("1.5MB", size.toString());
    Assert.assertEquals(1536L * 1024, size.getBytes());
    Assert.assertEquals("1.5MB", size.convertTo("MB"));
    Assert.assertEquals("1536KB", size.convertTo("KB"));
    Assert.assertEquals("1.5MB", size.convertTo("invalid")); // Default to original unit
  }

  @Test
  public void testTimeDurationConversion() {
    TimeDuration duration = new TimeDuration("1.5m");
    Assert.assertEquals("1.5m", duration.toString());
    Assert.assertEquals(90 * 1000L, duration.getMilliseconds());
    Assert.assertEquals("1.5m", duration.convertTo("m"));
    Assert.assertEquals("90s", duration.convertTo("s"));
    Assert.assertEquals("1.5m", duration.convertTo("invalid")); // Default to original unit
  }
} 

