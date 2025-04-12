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

package io.cdap.functions;

import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link DataQuality}
 */
public class DataQualityTest {

  @Test
  public void testRecordLength() throws Exception {
    Row row = new Row("a", 1).add("b", 2).add("c", 3);
    Assert.assertEquals(3, DataQuality.columns(row));
    row = new Row("a", 1);
    Assert.assertEquals(1, DataQuality.columns(row));
    row = new Row();
    Assert.assertEquals(0, DataQuality.columns(row));
  }

  @Test
  public void testRecordHasColumn() throws Exception {
    Row row = new Row("a", 1);
    Assert.assertEquals(true, DataQuality.hascolumn(row, "a"));
    Assert.assertEquals(false, DataQuality.hascolumn(row, "b"));
    row = new Row();
    Assert.assertEquals(false, DataQuality.hascolumn(row, "a"));
  }

  @Test
  public void testRange() throws Exception {
    Assert.assertEquals(true, DataQuality.inrange(1, 0, 10));
    Assert.assertEquals(false, DataQuality.inrange(0.9, 1, 10));
    Assert.assertEquals(true, DataQuality.inrange(1.1, 1, 10));
  }

}
