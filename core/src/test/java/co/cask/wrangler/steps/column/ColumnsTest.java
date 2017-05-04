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

package co.cask.wrangler.steps.column;

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.executor.TextDirectives;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link Columns}
 */
public class ColumnsTest {

  @Test(expected = DirectiveParseException.class)
  public void testEmptySetColumnsDirectiveAtStart() throws Exception {
    String[] directives = {
      "set columns ,A,B"
    };
    Directives d = new TextDirectives(directives);
    d.getSteps();
  }

  @Test(expected = DirectiveParseException.class)
  public void testEmptySetColumnsDirectiveInMiddle() throws Exception {
    String[] directives = {
      "set columns A,B, ,D"
    };
    Directives d = new TextDirectives(directives);
    d.getSteps();
  }

  @Test
  public void testEmptySetColumnsDirectiveAtEnd1() throws Exception {
    String[] directives = {
      "set columns A,B,D,"
    };
    Directives d = new TextDirectives(directives);
    List<Step> steps = d.getSteps();
    Assert.assertEquals(1, steps.size());
  }

  @Test
  public void testEmptySetColumnsDirectiveAtEnd2() throws Exception {
    String[] directives = {
      "set columns A,B,D,,"
    };
    Directives d = new TextDirectives(directives);
    d.getSteps();
  }

}
