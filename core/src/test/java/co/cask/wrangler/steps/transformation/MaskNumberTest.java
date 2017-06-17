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

import co.cask.wrangler.api.Directives;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.parser.TextDirectives;
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
    Step step = new MaskNumber(0, "", "ssn", "xxx-xx-#####");
    List<Record> actual = step.execute(Arrays.asList(new Record("ssn", "888990000")), null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));

    step = new MaskNumber(0, "", "ssn", "xxx-xx-####");
    actual = step.execute(Arrays.asList(new Record("ssn", "888-99-1234")), null);
    Assert.assertEquals("xxx-xx-1234", actual.get(0).getValue("ssn"));

    step = new MaskNumber(0, "", "ssn", "xxx-xx-####-0");
    actual = step.execute(Arrays.asList(new Record("ssn", "888990000")), null);
    Assert.assertEquals("xxx-xx-0000-0", actual.get(0).getValue("ssn"));

    step = new MaskNumber(0, "", "ssn", "xxx-xx-####");
    actual = step.execute(Arrays.asList(new Record("ssn", "888990000")), null);
    Assert.assertEquals("xxx-xx-0000", actual.get(0).getValue("ssn"));
    step = new MaskNumber(0, "", "ssn", "x-####");
    actual = step.execute(Arrays.asList(new Record("ssn", "888990000")), null);
    Assert.assertEquals("x-8899", actual.get(0).getValue("ssn"));
  }

  @Test
  public void testSSNWithDashesExact() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-xx-####"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "000-00-1234")
    );

    Directives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);

    Assert.assertEquals(1, records.size());
    Assert.assertEquals("xxx-xx-1234", records.get(0).getValue("body"));
  }

  @Test
  public void testSSNWithDashesExtra() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-xx-#####"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "000-00-1234")
    );

    Directives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);

    Assert.assertEquals(1, records.size());
    Assert.assertEquals("xxx-xx-1234", records.get(0).getValue("body"));
  }

  @Test
  public void testComplexMasking() throws Exception {
    String[] directives = new String[] {
      "mask-number body xxx-##-xx-##-XXXX-9"
    };

    List<Record> records = Arrays.asList(
      new Record("body", "0000012349898")
    );

    Directives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);

    Assert.assertEquals(1, records.size());
    Assert.assertEquals("xxx-00-xx-34-xxxx-9", records.get(0).getValue("body"));
  }
}
