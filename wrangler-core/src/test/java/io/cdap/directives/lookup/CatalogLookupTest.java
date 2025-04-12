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

package io.cdap.directives.lookup;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link CatalogLookup}
 */
public class CatalogLookupTest {

  @Test
  public void testICDCodeLookup() throws Exception {
    String[] directives = new String[] {
      "catalog-lookup icd-10-2016 code",
      "catalog-lookup ICD-10-2017 code",
    };


    List<Row> rows = Arrays.asList(
      new Row("code", "A0100"),
      new Row("code", "A0102"),
      new Row("code", "Z9989"),
      new Row("code", "Y36521S"),
      new Row("code", "ABC"),     // Invalid code.
      new Row("name", "Root")     // Code Column doesn't exit.
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 6);
    Assert.assertEquals("code_icd_10_2016_description", rows.get(0).getColumn(1));
    Assert.assertEquals("code_icd_10_2017_description", rows.get(0).getColumn(2));
    for (int i = 0; i < 6; ++i) {
      Assert.assertEquals(3, rows.get(i).width());
    }
  }

}
