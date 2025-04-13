/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

 package io.cdap.wrangler.parser.directives;

 import io.cdap.wrangler.TestingRig;
 import io.cdap.wrangler.api.RecipeException;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import org.junit.Assert;
 import org.junit.Test;
 
 import java.util.Arrays;
 import java.util.Collections;
 import java.util.List;
 
 public class AggregateStatsDirectiveTest {
 
     private static final double DELTA = 1e-6;
 
     @Test
     public void testBasicAggregation() throws Exception {
         String[] recipe = {
             "aggregate-stats :bytes :duration :total_mb :total_sec"
         };
 
         List<Row> input = Arrays.asList(
             new Row("bytes", new ByteSize("1024KB")).add("duration", new TimeDuration("500ms")),
             new Row("bytes", new ByteSize("2MB")).add("duration", new TimeDuration("1.5s")),
             new Row("bytes", new ByteSize("0.5MB")).add("duration", new TimeDuration("2000ms"))
         );
 
         double expectedMB = 3.5;
         double expectedSec = 4.0;
 
         List<Row> output = TestingRig.execute(recipe, input);
 
         Assert.assertEquals(1, output.size());
         Row result = output.get(0);
 
         Assert.assertEquals(expectedMB, (double) result.getValue("total_mb"), DELTA);
         Assert.assertEquals(expectedSec, (double) result.getValue("total_sec"), DELTA);
     }
 
     @Test
     public void testEmptyInput() throws Exception {
         String[] recipe = {
             "aggregate-stats :bytes :duration :total_mb :total_sec"
         };
 
         List<Row> output = TestingRig.execute(recipe, Collections.emptyList());
 
         Assert.assertEquals(1, output.size());
         Row result = output.get(0);
 
         Assert.assertEquals(0.0, (double) result.getValue("total_mb"), DELTA);
         Assert.assertEquals(0.0, (double) result.getValue("total_sec"), DELTA);
     }
 
     @Test
     public void testSingleRowInput() throws Exception {
         String[] recipe = {
             "aggregate-stats :bytes :duration :total_mb :total_sec"
         };
 
         List<Row> input = Collections.singletonList(
             new Row("bytes", new ByteSize("4MB")).add("duration", new TimeDuration("2500ms"))
         );
 
         double expectedMB = 4.0;
         double expectedSec = 2.5;
 
         List<Row> output = TestingRig.execute(recipe, input);
 
         Assert.assertEquals(1, output.size());
         Row result = output.get(0);
 
         Assert.assertEquals(expectedMB, (double) result.getValue("total_mb"), DELTA);
         Assert.assertEquals(expectedSec, (double) result.getValue("total_sec"), DELTA);
     }
 
     @Test(expected = RecipeException.class)
     public void testWrongByteTypeInInput() throws Exception {
         String[] recipe = {
             "aggregate-stats :bytes :duration :total_mb :total_sec"
         };
 
         List<Row> input = Arrays.asList(
             new Row("bytes", new ByteSize("1MB")).add("duration", new TimeDuration("1s")),
             new Row("bytes", "not a ByteSize").add("duration", new TimeDuration("1s"))
         );
 
         TestingRig.execute(recipe, input);
     }
 
     @Test(expected = RecipeException.class)
     public void testWrongTimeTypeInInput() throws Exception {
         String[] recipe = {
             "aggregate-stats :bytes :duration :total_mb :total_sec"
         };
 
         List<Row> input = Arrays.asList(
             new Row("bytes", new ByteSize("1MB")).add("duration", new TimeDuration("1s")),
             new Row("bytes", new ByteSize("1MB")).add("duration", 12345L)
         );
 
         TestingRig.execute(recipe, input);
     }
 
     @Test(expected = RecipeException.class)
     public void testMissingByteColumnInInput() throws Exception {
         String[] recipe = {
             "aggregate-stats :bytes :duration :total_mb :total_sec"
         };
 
         List<Row> input = Arrays.asList(
             new Row("bytes", new ByteSize("1MB")).add("duration", new TimeDuration("1s")),
             new Row("duration", new TimeDuration("1s"))
         );
 
         TestingRig.execute(recipe, input);
     }
    }
    
