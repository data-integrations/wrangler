/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */

 package io.cdap.wrangler.parser;

 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import org.junit.Test;
 
 import static org.junit.Assert.*;
 
 public class ByteSizeAndTimeDurationTest {
 
   @Test
   public void testByteSizeParsing() {
     ByteSize size1 = new ByteSize("10B");
     assertEquals(10L, size1.getBytes());
 
     ByteSize size2 = new ByteSize("1KB");
     assertEquals(1024L, size2.getBytes());
 
     ByteSize size3 = new ByteSize("1.5MB");
     assertEquals(1_572_864L, size3.getBytes());
 
     ByteSize size4 = new ByteSize("2GB");
     assertEquals(2L * 1024 * 1024 * 1024, size4.getBytes());
 
     ByteSize size5 = new ByteSize("1.2TB");
     assertEquals((long) (1.2 * 1024 * 1024 * 1024 * 1024), size5.getBytes());
   }
 
   @Test
   public void testTimeDurationParsing() {
     TimeDuration duration1 = new TimeDuration("150ms");
     assertEquals(150L, duration1.getDurationMillis());
 
     TimeDuration duration2 = new TimeDuration("2s");
     assertEquals(2000L, duration2.getDurationMillis());
 
     TimeDuration duration3 = new TimeDuration("1.5m");
     assertEquals((long) (1.5 * 60 * 1000), duration3.getDurationMillis());
 
     TimeDuration duration4 = new TimeDuration("2h");
     assertEquals(2L * 60 * 60 * 1000, duration4.getDurationMillis());
 
     TimeDuration duration5 = new TimeDuration("1.25d");
     assertEquals((long) (1.25 * 24 * 60 * 60 * 1000), duration5.getDurationMillis());
   }
 
   @Test
   public void testInvalidByteSize() {
     try {
       new ByteSize("10XYZ");
       fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) {
       assertTrue(e.getMessage().contains("Invalid byte size unit"));
     }
   }
 
   @Test
   public void testInvalidTimeDuration() {
     try {
       new TimeDuration("abc");
       fail("Expected IllegalArgumentException");
     } catch (IllegalArgumentException e) {
       assertTrue(e.getMessage().contains("Invalid time unit"));
     }
   }
 }
 