/*
 *  Copyright © 2017 Cask Data, Inc.
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

package co.cask.directives.row;

import co.cask.TestUtil;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link RecordRegexFilter}
 */
public class RowRegexFilterTest {
  @Test
  public void testFilterKeepDoesntKeepNullValues() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv :body ',' false",
      "filter-regex-match :body_3 '.*pot.*'"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1, \"Archil\", , \"SHAH\", 19, \"2017-06-02\""),
      new Row("body", "2, \"Sameet\", \"andpotatoes\", \"Sapra\", 19, \"2017-06-02\""),
      new Row("body", "3, \"Bob\", , \"Sagett\", 101, \"1970-01-01\"")
    );

    rows = TestUtil.execute(directives, rows);
    Assert.assertTrue(rows.size() == 1);
  }
}
