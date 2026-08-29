/*
 * Copyright © 2024 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ByteSize} token
 */
public class ByteSizeTest {

  @Test
  public void testByteSizeParsing() {
    // Test basic values
    validateByteSize("1024", 1024);
    validateByteSize("1KB", 1024);
    validateByteSize("1kb", 1024);
    validateByteSize("1MB", 1024 * 1024);
    validateByteSize("1mb", 1024 * 1024);
    validateByteSize("1GB", 1024 * 1024 * 1024);
    validateByteSize("1gb", 1024 * 1024 * 1024);
    validateByteSize("1TB", 1024L * 1024L * 1024L * 1024L);
    validateByteSize("1tb", 1024L * 1024L * 1024L * 1024L);

    // Test decimal values
    validateByteSize("1.5KB", (long) (1.5 * 1024));
    validateByteSize("2.5MB", (long) (2.5 * 1024 * 1024));
    validateByteSize("3.5GB", (long) (3.5 * 1024 * 1024 * 1024));
    
    // Test larger values
    validateByteSize("100MB", 100L * 1024 * 1024);
    validateByteSize("10GB", 10L * 1024 * 1024 * 1024);
  }

  private void validateByteSize(String value, long expectedBytes) {
    ByteSize byteSize = new ByteSize(value);
    Assert.assertEquals(value, byteSize.value());
    Assert.assertEquals(expectedBytes, byteSize.getBytes());
    Assert.assertEquals(TokenType.BYTE_SIZE, byteSize.type());
  }
}
