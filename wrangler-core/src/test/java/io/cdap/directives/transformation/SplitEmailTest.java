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
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link SplitEmail}
 */
public class SplitEmailTest {
  @Test
  public void testSplitEmail() throws Exception {
    String[] directives = new String[] {
      "split-email email",
    };

    List<Row> rows = Arrays.asList(
      new Row("email", "root@cask.co"),
      new Row("email", "joltie.xxx@gmail.com"),
      new Row("email", "joltie_xxx@hotmail.com"),
      new Row("email", "joltie.\"@.\"root.\"@\".@yahoo.com"),
      new Row("email", "Joltie, Root <joltie.root@hotmail.com>"),
      new Row("email", "Joltie,Root<joltie.root@hotmail.com>"),
      new Row("email", "Joltie,Root<joltie.root@hotmail.com") // bad email
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 7);

    Assert.assertEquals("root", rows.get(0).getValue("email_account"));
    Assert.assertEquals("cask.co", rows.get(0).getValue("email_domain"));

    Assert.assertEquals("joltie.xxx", rows.get(1).getValue("email_account"));
    Assert.assertEquals("gmail.com", rows.get(1).getValue("email_domain"));

    Assert.assertEquals("joltie_xxx", rows.get(2).getValue("email_account"));
    Assert.assertEquals("hotmail.com", rows.get(2).getValue("email_domain"));

    Assert.assertEquals("joltie.\"@.\"root.\"@\".", rows.get(3).getValue("email_account"));
    Assert.assertEquals("yahoo.com", rows.get(3).getValue("email_domain"));

    Assert.assertEquals("joltie.root", rows.get(4).getValue("email_account"));
    Assert.assertEquals("hotmail.com", rows.get(4).getValue("email_domain"));

    Assert.assertEquals("joltie.root", rows.get(5).getValue("email_account"));
    Assert.assertEquals("hotmail.com", rows.get(5).getValue("email_domain"));

    Assert.assertNull(rows.get(6).getValue("email_account"));
    Assert.assertNull(rows.get(6).getValue("email_domain"));
  }

  @Test(expected = RecipeException.class)
  public void testBadType() throws Exception {
    String[] directives = new String[] {
      "split-email email",
    };

    List<Row> rows = Arrays.asList(
      new Row("email", new Integer(1)) // Injecting bad type.
    );

    TestingRig.execute(directives, rows);
  }

  @Test
  public void testBadEmailId() throws Exception {
    String[] directives = new String[] {
      "split-email email",
    };

    List<Row> rows = Arrays.asList(
      new Row("email", "root@hotmail@com"),
      new Row("email", "root.hotmail.com"),
      new Row("email", ""),
      new Row("email", null)
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertTrue(rows.size() == 4);

    Assert.assertNotNull(rows.get(0).getValue("email_account"));
    Assert.assertNotNull(rows.get(0).getValue("email_domain"));
    Assert.assertNull(rows.get(1).getValue("email_account"));
  }
}
