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
  package io.cdap.wrangler;
 
  import io.cdap.wrangler.api.parser.ByteSize;
  import org.junit.Assert;
  import org.junit.Test;
  
  public class ByteSizeTest {
  
      @Test
      public void testByteSizeParsing() {
          Assert.assertEquals(1024, new ByteSize("1KB").getBytes());
          Assert.assertEquals(1536, new ByteSize("1.5KB").getBytes());
          Assert.assertEquals(1048576, new ByteSize("1MB").getBytes());
          Assert.assertEquals(1610612736, new ByteSize("1.5GB").getBytes());
          Assert.assertEquals(100, new ByteSize("100B").getBytes());
      }
  
      @Test(expected = IllegalArgumentException.class)
      public void testInvalidByteSize() {
          new ByteSize("invalid");
      }
  }