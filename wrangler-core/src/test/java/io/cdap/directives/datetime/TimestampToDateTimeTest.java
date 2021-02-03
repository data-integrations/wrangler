/*
 *  Copyright © 2021 Cask Data, Inc.
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
package io.cdap.directives.datetime;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;

public class TimestampToDateTimeTest {

  @Test
  public void testConversion() throws Exception {
    String colName = "col1";
    ZonedDateTime zonedDateTime = ZonedDateTime.now();
    Row row = new Row();
    String[] directives = new String[]{String.format("%s :%s", TimestampToDateTime.NAME, colName)};
    row.add(colName, zonedDateTime);
    List<Row> rows = TestingRig.execute(directives, Collections.singletonList(row));

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals(zonedDateTime.toLocalDateTime(), rows.get(0).getValue(colName));
  }

  @Test
  public void testInvalidObject() throws Exception {
    String colName = "col1";
    String[] directives = new String[]{String.format("%s :%s", TimestampToDateTime.NAME, colName)};
    Row row1 = new Row();
    row1.add(colName, LocalDateTime.now().toString());
    final List<Row> results = TestingRig.execute(directives, Collections.singletonList(row1));
    //should be error collected
    Assert.assertTrue(results.isEmpty());
  }
}
