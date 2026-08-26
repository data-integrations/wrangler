/*
 * Copyright © 2025 Garv Tayal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler.steps.transformation;

 import io.cdap.wrangler.api.Row;
 import org.junit.Assert;
 import org.junit.Test;
 
 import java.util.Arrays;
 import java.util.Collections;
 import java.util.List;
 
 public class AggregateStatsTest {
 
     @Test
     public void testAggregateStatsDirective() {
         List<Row> rows = Arrays.asList(
             new Row("value", 10),
             new Row("value", 20),
             new Row("value", 30)
         );
 
         AggregateStats directive = new AggregateStats("stats", "value");
         List<Row> output = directive.execute(rows, null);
 
         Row result = output.get(0);
         Assert.assertEquals(3L, result.getValue("stats_count"));
         Assert.assertEquals(60.0, result.getValue("stats_sum"));
         Assert.assertEquals(10.0, result.getValue("stats_min"));
         Assert.assertEquals(30.0, result.getValue("stats_max"));
         Assert.assertEquals(20.0, result.getValue("stats_avg"));
     }
 
     @Test(expected = RuntimeException.class)
     public void testInvalidColumn() {
         List<Row> rows = Collections.singletonList(new Row("wrong_column", 10));
         AggregateStats directive = new AggregateStats("stats", "value");
         directive.execute(rows, null);
     }
 }

