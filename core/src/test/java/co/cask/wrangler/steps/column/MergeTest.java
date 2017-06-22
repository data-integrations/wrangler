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

import co.cask.wrangler.TestUtil;
import co.cask.wrangler.api.pipeline.PipelineException;
import co.cask.wrangler.api.Record;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link Merge}
 */
public class MergeTest {

  @Test
  public void testBasicMergeFunctionality() throws Exception {
    String[] directives = new String[] {
      "merge A B C ','",
      "merge B A D ' '"
    };

    List<Record> records = Arrays.asList(
      new Record("A", "Root").add("B", "Joltie")
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("Root,Joltie", records.get(0).getValue("C"));
    Assert.assertEquals("Joltie Root", records.get(0).getValue("D"));
  }

  @Test
  public void testWithQuoteAsSeparator() throws Exception {
    String[] directives = new String[] {
      "merge A B C '''",
    };

    List<Record> records = Arrays.asList(
      new Record("A", "Root").add("B", "Joltie")
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("Root'Joltie", records.get(0).getValue("C"));
  }

  @Test
  public void testWithUTF8Separator() throws Exception {
    String[] directives = new String[] {
      "merge A B C '\\u000A'", // in actuality you need only one back slash.
    };

    List<Record> records = Arrays.asList(
      new Record("A", "Root").add("B", "Joltie")
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("Root\nJoltie", records.get(0).getValue("C"));
  }

  @Test (expected = PipelineException.class)
  public void testSingleQuoteAtStartOnly() throws Exception {
    String[] directives = new String[] {
      "merge A B C '\\u000A", // in actuality you need only one back slash.
    };

    List<Record> records = Arrays.asList(
      new Record("A", "Root").add("B", "Joltie")
    );

    TestUtil.run(directives, records);
  }

  @Test (expected = PipelineException.class)
  public void testSingleQuoteAtEndOnly() throws Exception {
    String[] directives = new String[] {
      "merge A B C \\u000A'", // in actuality you need only one back slash.
    };

    List<Record> records = Arrays.asList(
      new Record("A", "Root").add("B", "Joltie")
    );

    TestUtil.run(directives, records);
  }

  @Test
  public void testWithMultipleCharactersAsSeparator() throws Exception {
    String[] directives = new String[] {
      "merge A B C '---'", // in actuality you need only one back slash.
    };

    List<Record> records = Arrays.asList(
      new Record("A", "Root").add("B", "Joltie")
    );

    records = TestUtil.run(directives, records);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals("Root---Joltie", records.get(0).getValue("C"));
  }
}