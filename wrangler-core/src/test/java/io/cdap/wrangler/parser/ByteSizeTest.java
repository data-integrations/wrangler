/*
 * Copyright © [year] [Company or Author]
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

import org.junit.Assert;
import org.junit.Test;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.ByteSize;
import com.google.gson.JsonElement;

public class ByteSizeTest {

  @Test
  public void testByteSizeConversion() {
    ByteSize byteSize = new ByteSize("10KB");

    // Test conversion from string to bytes
    Assert.assertEquals(10240, byteSize.getBytes());  // 10 * 1024

    byteSize = new ByteSize("1.5MB");
    Assert.assertEquals(1572864, byteSize.getBytes()); // 1.5 * 1024 * 1024

    byteSize = new ByteSize("2GB");
    Assert.assertEquals(2147483648L, byteSize.getBytes()); // 2 * 1024 * 1024 * 1024

    byteSize = new ByteSize("5TB");
    Assert.assertEquals(5497558138880L, byteSize.getBytes()); // 5 * 1024 * 1024 * 1024 * 1024

    // Test edge case like 0
    byteSize = new ByteSize("0B");
    Assert.assertEquals(0, byteSize.getBytes());
  }

  @Test
  public void testInvalidByteSize() {
    // Test invalid byte size values
    try {
      new ByteSize("10XZ");
      Assert.fail("Expected IllegalArgumentException for invalid unit");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Invalid byte size format '10XZ'. Expected format is <number><unit> where unit is B, KB, MB, GB, or TB", e.getMessage());
    }
  }

  @Test
  public void testToStringRepresentation() {
    ByteSize byteSize = new ByteSize("1024KB");
    Assert.assertEquals("1024KB", byteSize.getOriginal());
  }

  @Test
  public void testJsonSerialization() {
    ByteSize byteSize = new ByteSize("10MB");
    JsonElement json = byteSize.toJson();
    
    // Validate the JSON structure
    Assert.assertEquals("BYTE_SIZE", json.getAsJsonObject().get("type").getAsString());
    Assert.assertEquals("10MB", json.getAsJsonObject().get("value").getAsString());
    Assert.assertEquals(10485760, json.getAsJsonObject().get("bytes").getAsLong()); // 10 * 1024 * 1024
  }
}
