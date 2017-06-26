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

import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.api.RecipePipeline;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.executor.RecipePipelineExecutor;
import co.cask.wrangler.parser.SimpleTextParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link MaskNumber}
 */
public class MaskNumberTest {

  @Test
  public void testOnlySteps() throws Exception {
    // More characters in mask, but not enough in the input.
    Directive directive = new MaskNumber(0, "", "ssn", "xxx-xx-#####");
    List<Row> actual = directive.execute(Arrays.asList(new Row("ssn", "888990000")), null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));

    directive = new MaskNumber(0, "", "ssn", "xxx-xx-####");
    actual = directive.execute(Arrays.asList(new Row("ssn", "888-99-1234")), null);
    Assert.assertEquals("xxx-xx-1234", actual.get(0).getValue("ssn"));

    directive = new MaskNumber(0, "", "ssn", "xxx-xx-####-0");
    actual = directive.execute(Arrays.asList(new Row("ssn", "888990000")), null);
    Assert.assertEquals("xxx-xx-0000-0", actual.get(0).getValue("ssn"));

    directive = new MaskNumber(0, "", "ssn", "xxx-xx-####");
    actual = directive.execute(Arrays.asList(new Row("ssn", "888990000")), null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));
    directive = new MaskNumber(0, "", "ssn", "x-####");
    actual = directive.execute(Arrays.asList(new Row("ssn", "888990000")), null);
    Assert.assertEquals("x-8899", actual.get(0).getValue("ssn"));
  }

  @Test
  public void testSSNWithDashesExact() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-xx-####"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "000-00-1234")
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xxx-xx-1234", rows.get(0).getValue("body"));
  }

  @Test
  public void testSSNWithDashesExtra() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-xx-#####"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "000-00-1234")
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xxx-xx-1234", rows.get(0).getValue("body"));
  }

  @Test
  public void testComplexMasking() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-##-xx-##-XXXX-9"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", "0000012349898")
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xxx-00-xx-34-xxxx-9", rows.get(0).getValue("body"));
  }

  @Test
  public void testIntegerTypeMasking() throws Exception {
    String[] directives = new String[] {
      "mask-number body xx-xx-#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12345),
      new Row("body", 123),
      new Row("body", 123456)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(3, rows.size());
    Assert.assertEquals("xx-xx-5", rows.get(0).getValue("body"));
    Assert.assertEquals("xx-xx-", rows.get(1).getValue("body"));
    Assert.assertEquals("xx-xx-5", rows.get(2).getValue("body"));
  }

  @Test
  public void testWithOtherCharacters() throws Exception {
    String[] directives = new String[] {
      "mask-number body xx-xx-TESTING-#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12345)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xx-xx-TESTING-5", rows.get(0).getValue("body"));
  }

  @Test
  public void testWithLong() throws Exception {
    String[] directives = new String[] {
      "mask-number body xx-xx-#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12345L)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("xx-xx-5", rows.get(0).getValue("body"));
  }

  @Test
  public void testWithFloat() throws Exception {
    String[] directives = new String[] {
      "mask-number body x#.x#"
    };

    List<Row> rows = Arrays.asList(
      new Row("body", 12.34)
    );

    RecipeParser d = new SimpleTextParser(directives);
    RecipePipeline pipeline = new RecipePipelineExecutor();
    pipeline.configure(d, null);
    rows = pipeline.execute(rows);

    Assert.assertEquals(1, rows.size());
    Assert.assertEquals("x2.x4", rows.get(0).getValue("body"));
  }
}

