/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.internal;

import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.steps.Columns;
import co.cask.wrangler.steps.CsvParser;
import co.cask.wrangler.steps.Drop;
import co.cask.wrangler.steps.Rename;
import org.apache.hadoop.util.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link TextDirectives} class.
 */
public class TextDirectivesTest {

  private static final String[] commands = new String[] {
    "set format csv ,    true",
    "set columns    a,b,c,d,e,f,g",
    "rename a first",
    "drop b"
  };

  @Test
  public void testBasicSpecification() throws Exception {
    Directives directives =
      new TextDirectives(StringUtils.join("\n", commands));
    List<Step> steps = directives.getSteps();
    Assert.assertEquals(5, steps.size());
    Assert.assertEquals(CsvParser.class, steps.get(0).getClass());
    Assert.assertEquals(Drop.class, steps.get(1).getClass());
    Assert.assertEquals(Columns.class, steps.get(2).getClass());
    Assert.assertEquals(Rename.class, steps.get(3).getClass());
    Assert.assertEquals(Drop.class, steps.get(4).getClass());
  }

  @Test
  public void testParsingFailures() throws Exception {
    int errorcount = 0;
    String[] errors = new String[] {
      "set format",
      "set format csv",
      "rename a",
      "rename a b", // good
      "drop",
      "drop col2", // good
      "uppercase",
      "titlecase",
      "indexsplit",
      "indexsplit col1",
      "indexsplit col1 1",
      "indexsplit col1 1 2",
      "indexsplit col1 1 2 dest1", // good
      "filter-row-if-true",
      "format-unix-timestamp col"
    };

    for (final String error : errors) {
      String[] err = { error };
      Directives directives =
        new TextDirectives(StringUtils.join("\n", err));
      try {
        List<Step> steps = directives.getSteps();
      } catch (DirectiveParseException e) {
        errorcount++;
      }
    }
    Assert.assertEquals(12, errorcount);
  }
}
