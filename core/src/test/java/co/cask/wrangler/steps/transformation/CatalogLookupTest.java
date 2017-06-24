/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.steps.transformation;

import co.cask.wrangler.api.Record;
import co.cask.wrangler.steps.RecipePipelineTest;
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


    List<Record> records = Arrays.asList(
      new Record("code", "A0100"),
      new Record("code", "A0102"),
      new Record("code", "Z9989"),
      new Record("code", "Y36521S"),
      new Record("code", "ABC"),     // Invalid code.
      new Record("name", "Root")     // Code Column doesn't exit.
    );

    records = RecipePipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 6);
    Assert.assertEquals("code_icd_10_2016_description", records.get(0).getColumn(1));
    Assert.assertEquals("code_icd_10_2017_description", records.get(0).getColumn(2));
    for (int i = 0; i < 6; ++i) {
      Assert.assertEquals(3, records.get(i).length());
    }
  }

}