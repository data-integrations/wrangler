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
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.steps.RecipePipelineTest;
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

    List<Record> records = Arrays.asList(
      new Record("email", "root@cask.co"),
      new Record("email", "joltie.xxx@gmail.com"),
      new Record("email", "joltie_xxx@hotmail.com"),
      new Record("email", "joltie.\"@.\"root.\"@\".@yahoo.com"),
      new Record("email", "Joltie, Root <joltie.root@hotmail.com>"),
      new Record("email", "Joltie,Root<joltie.root@hotmail.com>"),
      new Record("email", "Joltie,Root<joltie.root@hotmail.com") // bad email
    );

    records = RecipePipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 7);

    Assert.assertEquals("root", records.get(0).getValue("email_account"));
    Assert.assertEquals("cask.co", records.get(0).getValue("email_domain"));

    Assert.assertEquals("joltie.xxx", records.get(1).getValue("email_account"));
    Assert.assertEquals("gmail.com", records.get(1).getValue("email_domain"));

    Assert.assertEquals("joltie_xxx", records.get(2).getValue("email_account"));
    Assert.assertEquals("hotmail.com", records.get(2).getValue("email_domain"));

    Assert.assertEquals("joltie.\"@.\"root.\"@\".", records.get(3).getValue("email_account"));
    Assert.assertEquals("yahoo.com", records.get(3).getValue("email_domain"));

    Assert.assertEquals("joltie.root", records.get(4).getValue("email_account"));
    Assert.assertEquals("hotmail.com", records.get(4).getValue("email_domain"));

    Assert.assertEquals("joltie.root", records.get(5).getValue("email_account"));
    Assert.assertEquals("hotmail.com", records.get(5).getValue("email_domain"));

    Assert.assertNull(records.get(6).getValue("email_account"));
    Assert.assertNull(records.get(6).getValue("email_domain"));
  }

  @Test(expected = DirectiveExecutionException.class)
  public void testBadType() throws Exception {
    String[] directives = new String[] {
      "split-email email",
    };

    List<Record> records = Arrays.asList(
      new Record("email", new Integer(1)) // Injecting bad type.
    );

    records = RecipePipelineTest.execute(directives, records);
  }

  @Test
  public void testBadEmailId() throws Exception {
    String[] directives = new String[] {
      "split-email email",
    };

    List<Record> records = Arrays.asList(
      new Record("email", "root@hotmail@com"),
      new Record("email", "root.hotmail.com"),
      new Record("email", ""),
      new Record("email", null)
    );

    records = RecipePipelineTest.execute(directives, records);
    Assert.assertTrue(records.size() == 4);

    Assert.assertNotNull(records.get(0).getValue("email_account"));
    Assert.assertNotNull(records.get(0).getValue("email_domain"));
    Assert.assertNull(records.get(1).getValue("email_account"));
  }
}
