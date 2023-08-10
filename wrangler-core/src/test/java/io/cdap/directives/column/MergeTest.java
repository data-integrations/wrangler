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

package io.cdap.directives.column;

import io.cdap.MockEngine;
import io.cdap.MockExpression;
import io.cdap.MockRelation;
import io.cdap.MockRelationalTransformContext;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.RecipeException;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.cdap.RelationalDirectiveTest.runTransform;

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

    List<Row> rows = Arrays.asList(
      new Row("A", "Root").add("B", "Joltie")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("Root,Joltie", rows.get(0).getValue("C"));
    Assert.assertEquals("Joltie Root", rows.get(0).getValue("D"));
  }

  @Ignore
  @Test
  public void testWithQuoteAsSeparator() throws Exception {
    String[] directives = new String[] {
      "merge A B C '\''",
    };

    List<Row> rows = Arrays.asList(
      new Row("A", "Root").add("B", "Joltie")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("Root'Joltie", rows.get(0).getValue("C"));
  }

  @Test
  public void testWithUTF8Separator() throws Exception {
    String[] directives = new String[] {
      "merge A B C '\\u000A'", // in actuality you need only one back slash.
    };

    List<Row> rows = Arrays.asList(
      new Row("A", "Root").add("B", "Joltie")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("Root\nJoltie", rows.get(0).getValue("C"));
  }

  @Ignore
  @Test
  public void testSingleQuoteAtEndOnly() throws Exception {
    String[] directives = new String[] {
      "merge A B C '\\u000A", // in actuality you need only one back slash.
    };

    List<Row> rows = Arrays.asList(
      new Row("A", "Root").add("B", "Joltie")
    );

    TestingRig.execute(directives, rows);
  }

  @Test
  public void testWithMultipleCharactersAsSeparator() throws Exception {
    String[] directives = new String[] {
      "merge A B C '---'", // in actuality you need only one back slash.
    };

    List<Row> rows = Arrays.asList(
      new Row("A", "Root").add("B", "Joltie")
    );

    rows = TestingRig.execute(directives, rows);
    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("Root---Joltie", rows.get(0).getValue("C"));
  }

  @Test
  public void testRelationColumn() throws DirectiveParseException, RecipeException {
    MockRelation relation = new MockRelation(null, null);
    Engine engine = new MockEngine();
    RelationalTranformContext relationalTranformContext = new MockRelationalTransformContext(engine, null, null,
            null, null);
    String[] recipe = {"merge testColumn1 testColumn2 destColumn ','"};
    Relation relation1 = runTransform(recipe, relationalTranformContext, relation);
    Assert.assertEquals(((MockRelation) relation1).getColumn(), "destColumn");
  }

  @Test
  public void testRelationExpression() throws DirectiveParseException, RecipeException {
    MockRelation relation = new MockRelation(null, null);
    Engine engine = new MockEngine();
    RelationalTranformContext relationalTranformContext = new MockRelationalTransformContext(engine, null, null,
            null, null);
    String[] recipe = {"merge testColumn1 testColumn2 destColumn ','"};
    Relation relation1 = runTransform(recipe, relationalTranformContext, relation);
    Assert.assertEquals(((MockExpression) ((MockRelation) relation1).getExpression()).getExpression(),
            "CONCAT(testColumn1,',',testColumn2)");
  }
}
