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

package co.cask.wrangler;

import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link Precondition}
 */
public class PreconditionTest {

  @Test
  public void testPrecondition() throws Exception {
    Row row = new Row("a", 1).add("b", "x").add("c", 2.06);
    Assert.assertEquals(true, new Precondition("a == 1 && b == \"x\"").apply(row));
    Assert.assertEquals(true, new Precondition("c > 2.0").apply(row));
    Assert.assertEquals(true, new Precondition("true").apply(row));
    Assert.assertEquals(false, new Precondition("false").apply(row));
  }

  @Test(expected = PreconditionException.class)
  public void testBadCondition() throws Exception {
    Row row = new Row("a", 1).add("b", "x").add("c", 2.06);
    Assert.assertEquals(true, new Precondition("c").apply(row));
  }
}
