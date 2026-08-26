/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

 package io.cdap.wrangler.api.parser;

 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import org.junit.Assert;
 import org.junit.Test;
 
 /**
  * Unit tests for ByteSize and TimeDuration classes.
  */
 public class ByteSizeAndTimeDurationTest {
 
     @Test
     public void testByteSizeParsing() {
         // Test parsing valid inputs to canonical byte units
         Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
         Assert.assertEquals(1048576L, new ByteSize("1MB").getBytes());
         Assert.assertEquals(1073741824L, new ByteSize("1GB").getBytes());
         Assert.assertEquals(10, new ByteSize("10B").getBytes());
 
         // Test for case insensitivity
         Assert.assertEquals(1572864L, new ByteSize("1.5MB").getBytes());
 
     }
 
     @Test
     public void testTimeDurationParsing() {
         // Test parsing valid inputs to canonical time units (milliseconds as double)
         double delta = 0.0001; // Tolerance for double comparison
 
         // 5ms -> 5.0 ms
         Assert.assertEquals(5.0, new TimeDuration("5ms").getValue(), delta);
 
         // 2.1s -> 2.1 * 1000.0 = 2100.0 ms
         Assert.assertEquals(2100.0, new TimeDuration("2.1s").getValue(), delta);
 
         // 1h -> 1.0 * 60.0 * 60.0 * 1000.0 = 3,600,000.0 ms
         Assert.assertEquals(3600000.0, new TimeDuration("1h").getValue(), delta);
 
         // Test for case insensitivity (using "min")
         // 1.5min -> 1.5 * 60.0 * 1000.0 = 90,000.0 ms
         Assert.assertEquals(90000.0, new TimeDuration("1.5min").getValue(), delta);
 
         // Test other units (assuming they were added to TimeDuration)
         // 1000us -> 1000.0 / 1000.0 = 1.0 ms
         Assert.assertEquals(1.0, new TimeDuration("1000us").getValue(), delta);
         // 5000000ns -> 5000000.0 / 1000000.0 = 5.0 ms
         Assert.assertEquals(5.0, new TimeDuration("5000000ns").getValue(), delta);
         // 1d -> 1.0 * 24.0 * 60.0 * 60.0 * 1000.0 = 86,400,000.0 ms
         Assert.assertEquals(86400000.0, new TimeDuration("1d").getValue(), delta);
     }
 
 } 