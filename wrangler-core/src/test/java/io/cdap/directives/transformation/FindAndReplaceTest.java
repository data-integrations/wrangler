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
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link FindAndReplace}
 */
public class FindAndReplaceTest {

  @Test
  public void testBasicReplacement() throws Exception {
    String[] directives = new String[] {
      "find-and-replace body s/hello/world/g",
    };

    Row row1 = new Row();
    row1.add("body", "hello");
    
    Row row2 = new Row();
    row2.add("body", "hello hello");
    
    Row row3 = new Row();
    row3.add("body", "goodbye hello");
    
    Row row4 = new Row();
    row4.add("body", "hello there");

    List<Row> rows = Arrays.asList(row1, row2, row3, row4);

    rows = TestingRig.execute(directives, rows);

    Assert.assertEquals(4, rows.size());
    Assert.assertEquals("world", rows.get(0).getValue("body"));
    Assert.assertEquals("world world", rows.get(1).getValue("body"));
    Assert.assertEquals("goodbye world", rows.get(2).getValue("body"));
    Assert.assertEquals("world there", rows.get(3).getValue("body"));
  }

  @Test
  public void testMultipleColumns() throws Exception {
    String[] directives = new String[] {
      "find-and-replace body,title s/test/exam/g",
    };

    Row row1 = new Row();
    row1.add("body", "this is a test");
    row1.add("title", "test title");
    
    Row row2 = new Row();
    row2.add("body", "another test here");
    row2.add("title", "title with test");
    
    Row row3 = new Row();
    row3.add("body", "no match");
    row3.add("title", "no match");

    List<Row> rows = Arrays.asList(row1, row2, row3);

    rows = TestingRig.execute(directives, rows);
    
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("this is a exam", rows.get(0).getValue("body"));
    Assert.assertEquals("exam title", rows.get(0).getValue("title"));
    Assert.assertEquals("another exam here", rows.get(1).getValue("body"));
    Assert.assertEquals("title with exam", rows.get(1).getValue("title"));
    Assert.assertEquals("no match", rows.get(2).getValue("body"));
    Assert.assertEquals("no match", rows.get(2).getValue("title"));
  }
}
