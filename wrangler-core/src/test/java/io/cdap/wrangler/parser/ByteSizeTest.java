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

 /**
  * Tests for the ByteSize token.
  */
 public class ByteSizeTest {

     /**
      * Verifies parsing of valid byte size units.
      * Note: Decimal values (e.g., "1.5MB") are not supported in this version.
      */
     @Test
     public void testByteParsing() {
         Assert.assertEquals(1024L, new ByteSize("1KB").getBytes());
         Assert.assertEquals(1048576L, new ByteSize("1MB").getBytes());
         Assert.assertEquals(1073741824L, new ByteSize("1GB").getBytes());
         Assert.assertEquals(1L, new ByteSize("1B").getBytes());
     }

     /**
      * Ensures an exception is thrown for invalid units.
      */
     @Test(expected = IllegalArgumentException.class)
     public void testInvalidUnit() {
         new ByteSize("5XY").getBytes();
     }
 }
