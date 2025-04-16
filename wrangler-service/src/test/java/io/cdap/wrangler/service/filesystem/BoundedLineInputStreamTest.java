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

package io.cdap.wrangler.service.filesystem;

import io.cdap.wrangler.service.explorer.BoundedLineInputStream;
import io.cdap.wrangler.service.explorer.Explorer;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests {@link BoundedLineInputStream}
 */
public class BoundedLineInputStreamTest {

  @Test
  public void testBasicLineReading() throws Exception {
    InputStream stream = Explorer.class.getClassLoader().getResourceAsStream("file.extensions");
    BoundedLineInputStream blis = BoundedLineInputStream.iterator(stream, "utf-8", 10);
    int i = 0;
    List<String> lines = new ArrayList<>();
    try {
      while (blis.hasNext()) {
        String line = blis.next();
        lines.add(line);
        Assert.assertNotNull(line);
        i++;
      }
    } finally {
      blis.close();
    }
    Assert.assertTrue(i == 10);
  }
}
