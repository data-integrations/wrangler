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

package co.cask.directives.column;

import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.RecipeException;
import co.cask.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Tests {@link SetHeader}
 */
public class SetHeaderTest {

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveAtStart() throws Exception {
    String[] directives = {
      "set-header ,A,B"
    };
    TestingRig.execute(directives, new ArrayList<Row>());
  }

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveInMiddle() throws Exception {
    String[] directives = {
      "set-header A,B, ,D"
    };
    TestingRig.execute(directives, new ArrayList<Row>());
  }

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveAtEnd1() throws Exception {
    String[] directives = {
      "set-header A,B,D,"
    };
    TestingRig.execute(directives, new ArrayList<Row>());
    Assert.assertTrue(true);
  }

  @Test(expected = RecipeException.class)
  public void testEmptySetColumnsDirectiveAtEnd2() throws Exception {
    String[] directives = {
      "set-header A,B,D,,"
    };
    TestingRig.execute(directives, new ArrayList<Row>());
    Assert.assertTrue(true);
  }

}
