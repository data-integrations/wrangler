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

package io.cdap.directives.transformation;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Encode}
 */
public class EncodeDecodeTest {
  @Test
  public void testEncodeDecode() throws Exception {
    String[] directives = new String[] {
      "encode base32 col1",
      "encode base64 col2",
      "encode hex col3"
    };

    List<Row> rows = Arrays.asList(
      new Row("col1", "Base32 Encoding").add("col2", "Testing Base 64 Encoding").add("col3", "Hex Encoding")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("IJQXGZJTGIQEK3TDN5SGS3TH", rows.get(0).getValue(3));
    Assert.assertEquals("VGVzdGluZyBCYXNlIDY0IEVuY29kaW5n", rows.get(0).getValue(4));
    Assert.assertEquals("48657820456e636f64696e67", rows.get(0).getValue(5));

    directives = new String[] {
      "decode base32 col1_encode_base32",
      "decode base64 col2_encode_base64",
      "decode hex col3_encode_hex"
    };

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals("Base32 Encoding", rows.get(0).getValue(6));
    Assert.assertEquals("Testing Base 64 Encoding", rows.get(0).getValue(7));
    Assert.assertEquals("Hex Encoding", rows.get(0).getValue(8));

  }
}
