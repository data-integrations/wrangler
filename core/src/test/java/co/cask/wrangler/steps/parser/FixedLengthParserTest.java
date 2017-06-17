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

package co.cask.wrangler.steps.parser;

import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.Pipeline;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.executor.PipelineExecutor;
import co.cask.wrangler.parser.TextDirectives;
import co.cask.wrangler.steps.PipelineTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link FixedLengthParser}
 */
public class FixedLengthParserTest {

  @Test
  public void testMismatchedLength() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body 2,2,1,1,3,4",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDEEEFFF")
    );

    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);
    List<Record> errors = pipeline.errors();
    Assert.assertTrue(records.size() == 0);
    Assert.assertEquals(1, errors.size());
  }

  @Test
  public void testFixedLengthParser() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body 2,2,1,1,3,4",
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AABBCDEEEFFFF")
    );

    TextDirectives d = new TextDirectives(directives);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(d, null);
    records = pipeline.execute(records);
    List<Record> errors = pipeline.errors();
    Assert.assertEquals(1, records.size());
    Assert.assertEquals(0, errors.size());
    Assert.assertEquals("AA", records.get(0).getValue("body_1"));
    Assert.assertEquals("BB", records.get(0).getValue("body_2"));
    Assert.assertEquals("C", records.get(0).getValue("body_3"));
    Assert.assertEquals("D", records.get(0).getValue("body_4"));
    Assert.assertEquals("EEE", records.get(0).getValue("body_5"));
    Assert.assertEquals("FFFF", records.get(0).getValue("body_6"));
  }

  @Test(expected = DirectiveParseException.class)
  public void testFixedLengthParserBadRangeSpecification() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body A-B,C-D,12",
    };

    TextDirectives specification = new TextDirectives(directives);
    List<Step> steps = new ArrayList<>();
    steps.addAll(specification.getSteps());
  }

  @Test
  public void testFixedLengthWidthPadding() throws Exception {
    String[] directives = new String[] {
      "parse-as-fixed-length body 4,4,4,4,4,4 '_'" ,
    };

    List<Record> records = Arrays.asList(
      new Record("body", "AA__BB__C___D___EEE_FFFF")
    );

    records = PipelineTest.execute(directives, records);

    Assert.assertTrue(records.size() == 1);
    Assert.assertEquals("AA", records.get(0).getValue("body_1"));
    Assert.assertEquals("BB", records.get(0).getValue("body_2"));
    Assert.assertEquals("C", records.get(0).getValue("body_3"));
    Assert.assertEquals("D", records.get(0).getValue("body_4"));
    Assert.assertEquals("EEE", records.get(0).getValue("body_5"));
    Assert.assertEquals("FFFF", records.get(0).getValue("body_6"));
  }

  @Test
  public void testFixedLengthComprehensive() throws Exception {
    int[] lengths = new int[] {
      1,9,20,12,1,2,1,3,14,14,14,14,15,9,2,3,8,4,8,7,4,11,14,14,2,1,1,1,1,1,1,1,1,6,6,6,42,1
    };

    List<Record> records = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    for (int r = 0; r < 20; r++) {
      for (int i = 0; i < lengths.length; ++i) {
        sb.append(String.format("%1$" + lengths[i] + "s", "x"));
      }
    }
    records.add(new Record("body", sb.toString()));

    String[] d = new String[] {
      "parse-as-fixed-length body 1,9,20,12,1,2,1,3,14,14,14,14,15,9,2,3,8,4,8,7,4,11,14,14,2,1,1,1,1,1,1,1,1,6,6,6,42,1 ' '",
      "drop body",
      "set columns A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,Z,W,X,Y,Z,A1,B1,C1,D1,E1,F1,G1,H1,I1,J1,K1,L1"
    };

    // Configure and parse directives.
    TextDirectives directives = new TextDirectives(d);
    Pipeline pipeline = new PipelineExecutor();
    pipeline.configure(directives, null);

    // Execute the pipeline.
    List<Record> results = pipeline.execute(records);
    List<Record> errors = pipeline.errors();

    Assert.assertEquals(20, results.size());
    Assert.assertEquals(0, errors.size());
  }

  public static String fixedLengthString(String string, int length) {
    return String.format("%1$"+length+ "s", string);
  }

}