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

package co.cask.wrangler.directives;

import co.cask.wrangler.test.api.TestRecipe;
import co.cask.wrangler.test.api.TestRows;
import co.cask.wrangler.test.TestingRig;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link TextReverse}
 */
public class TextReverseTest {

  @Test
  public void testBasicReverse() throws Exception {
    TestRecipe recipe = new TestRecipe();
    recipe.add("parse-as-csv :body ';';");
    recipe.add("set-headers :a,:b,:c;");
    recipe.add("text-reverse :b");

    TestRows rows = new TestRows();
    rows.add(new Row("body", "root,joltie,mars avenue"));
    rows.add(new Row("body", "joltie,root,venus blvd"));

    RecipePipeline pipeline = TestingRig.pipeline(TextReverse.class, recipe);
    List<Row> actual = pipeline.execute(rows.toList());

    Assert.assertEquals(2, actual.size());
    Assert.assertEquals("eitloj", actual.get(0).getValue("b"));
    Assert.assertEquals("toor", actual.get(0).getValue("b"));
  }
}