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
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.cdap.RelationalDirectiveTest.runTransform;

/**
 * Tests {@link Keep}
 */
public class KeepTest {

  @Test
  public void testKeep() throws Exception {
    String[] directives = new String[] {
      "parse-as-csv body ,",
      "keep body_1,body_2"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "1,2,3,4,5,6,7,8,9,10")
    );

    rows = TestingRig.execute(directives, rows);

    Assert.assertTrue(rows.size() == 1);
    Assert.assertEquals(2, rows.get(0).width());
  }

  @Test
  public void testRelationColumn() throws DirectiveParseException, RecipeException {
    MockRelation relation = new MockRelation(null, null);
    Engine engine = new MockEngine();
    RelationalTranformContext relationalTranformContext = new MockRelationalTransformContext(engine, null, null,
            null, null);
    String[] recipe = {"keep column1"};
    Relation relation1 = runTransform(recipe, relationalTranformContext, relation);
    Assert.assertEquals(((MockRelation) relation1).getColumn(), "column1");
  }

  @Test
  public void testRelationExpression() throws DirectiveParseException, RecipeException {
    MockRelation relation = new MockRelation(null, null);
    Engine engine = new MockEngine();
    RelationalTranformContext relationalTranformContext = new MockRelationalTransformContext(engine, null, null,
            null, null);
    String[] recipe = {"keep column1"};
    Relation relation1 = runTransform(recipe, relationalTranformContext, relation);
    Assert.assertEquals(((MockExpression) ((MockRelation) relation1).getExpression()).getExpression(),
            "column1");
  }

  @Test
  public void testMultipleColumns() throws DirectiveParseException, RecipeException {
    MockRelation relation = new MockRelation(null, null);
    Engine engine = new MockEngine();
    RelationalTranformContext relationalTranformContext = new MockRelationalTransformContext(engine, null, null,
            null, null);
    String[] recipe = {"keep column1,column2,column3"};
    Relation relation1 = runTransform(recipe, relationalTranformContext, relation);
      String[] outputColumns = ((MockExpression) ((MockRelation) relation1)
                        .getExpression()).getExpression().split(",");
    Assert.assertEquals(outputColumns.length, 3);
  }

}
