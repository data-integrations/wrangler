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
   public void testParseByteSize() {
     // Test basic parsing
     ByteSize size1 = new ByteSize("10KB");
     Assert.assertEquals(10 * 1024L, size1.value().longValue());
     Assert.assertEquals(10.0, size1.getValue("KB"), 0.001);
 
     ByteSize size2 = new ByteSize("1.5MB");
     Assert.assertEquals((long)(1.5 * 1024 * 1024), size2.value().longValue());
     Assert.assertEquals(1.5, size2.getValue("MB"), 0.001);
 
     // Test different unit formats
     ByteSize size3 = new ByteSize("2G");
     Assert.assertEquals(2L * 1024 * 1024 * 1024, size3.value().longValue());
     Assert.assertEquals(2.0, size3.getValue("GB"), 0.001);
 
     // Test bytes
     ByteSize size4 = new ByteSize("1024B");
     Assert.assertEquals(1024L, size4.value().longValue());
     Assert.assertEquals(1024.0, size4.getValue("B"), 0.001);
 
     // Test large values
     ByteSize size5 = new ByteSize("1PB");
     Assert.assertEquals(1L * 1024 * 1024 * 1024 * 1024 * 1024, size5.value().longValue());
     Assert.assertEquals(1.0, size5.getValue("PB"), 0.001);
   }
 
   @Test(expected = IllegalArgumentException.class)
   public void testInvalidFormat() {
     new ByteSize("invalid");
   }
 
   @Test(expected = IllegalArgumentException.class)
   public void testInvalidUnit() {
     new ByteSize("10XX");
   }
 
   @Test
   public void testEmptyValue() {
     ByteSize size = new ByteSize("");
     Assert.assertEquals(0L, size.value().longValue());
   }
 
   @Test
   public void testUnitConversion() {
     ByteSize size = new ByteSize("1MB");
     
     // Test conversion to different units
     Assert.assertEquals(1024.0, size.getValue("KB"), 0.001);
     Assert.assertEquals(1.0, size.getValue("MB"), 0.001);
     Assert.assertEquals(0.0009765625, size.getValue("GB"), 0.001);
     Assert.assertEquals(1048576.0, size.getValue("B"), 0.001);
   }
 } 