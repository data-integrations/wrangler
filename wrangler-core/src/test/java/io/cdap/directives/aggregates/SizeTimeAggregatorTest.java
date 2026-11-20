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

 package io.cdap.directives.aggregates;

 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Text;
 import io.cdap.wrangler.api.parser.TokenType;
 import io.cdap.wrangler.api.parser.UsageDefinition;
 import io.cdap.wrangler.parser.MapArguments;
 import io.cdap.wrangler.api.TokenGroup;
 import io.cdap.wrangler.api.SourceInfo;
 import org.junit.Assert;
 import org.junit.Test;
 
 import java.util.Arrays;
 import java.util.List;
 
 public class SizeTimeAggregatorTest {
 
   @Test
   public void testTotalAggregation() throws Exception {
     List<Row> rows = Arrays.asList(
       createRow("1MB", "100ms"),
       createRow("2MB", "200ms"),
       createRow("3MB", "300ms")
     );
 
     SizeTimeAggregator directive = new SizeTimeAggregator();
     
     // Create UsageDefinition
     UsageDefinition.Builder builder = UsageDefinition.builder("size-time-aggregator");
     builder.define("size-column", TokenType.COLUMN_NAME);
     builder.define("time-column", TokenType.COLUMN_NAME);
     builder.define("target-size-column", TokenType.COLUMN_NAME);
     builder.define("target-time-column", TokenType.COLUMN_NAME);
     
     // Create TokenGroup
     TokenGroup tokenGroup = new TokenGroup(new SourceInfo(1, 1, "test"));
     tokenGroup.add(new ColumnName("size"));
     tokenGroup.add(new ColumnName("time"));
     tokenGroup.add(new ColumnName("total_size"));
     tokenGroup.add(new ColumnName("total_time"));
     
     // Create MapArguments
     MapArguments args = new MapArguments(builder.build(), tokenGroup);
     directive.initialize(args);
 
     List<Row> results = directive.execute(rows, null);
     Assert.assertEquals(1, results.size());
     
     Row result = results.get(0);
     Assert.assertEquals(6.0, (Double) result.getValue("total_size"), 0.001); // 6MB total
     Assert.assertEquals(0.6, (Double) result.getValue("total_time"), 0.001); // 600ms = 0.6s
   }
 
   @Test
   public void testAverageAggregation() throws Exception {
     List<Row> rows = Arrays.asList(
       createRow("1MB", "100ms"),
       createRow("2MB", "200ms"),
       createRow("3MB", "300ms")
     );
 
     SizeTimeAggregator directive = new SizeTimeAggregator();
     
     // Create UsageDefinition
     UsageDefinition.Builder builder = UsageDefinition.builder("size-time-aggregator");
     builder.define("size-column", TokenType.COLUMN_NAME);
     builder.define("time-column", TokenType.COLUMN_NAME);
     builder.define("target-size-column", TokenType.COLUMN_NAME);
     builder.define("target-time-column", TokenType.COLUMN_NAME);
     builder.define("aggregation-type", TokenType.TEXT);
     
     // Create TokenGroup
     TokenGroup tokenGroup = new TokenGroup(new SourceInfo(1, 1, "test"));
     tokenGroup.add(new ColumnName("size"));
     tokenGroup.add(new ColumnName("time"));
     tokenGroup.add(new ColumnName("avg_size"));
     tokenGroup.add(new ColumnName("avg_time"));
     tokenGroup.add(new Text("average"));
     
     // Create MapArguments
     MapArguments args = new MapArguments(builder.build(), tokenGroup);
     directive.initialize(args);
 
     List<Row> results = directive.execute(rows, null);
     Assert.assertEquals(1, results.size());
     
     Row result = results.get(0);
     Assert.assertEquals(2.0, (Double) result.getValue("avg_size"), 0.001); // 6MB / 3 = 2MB average
     Assert.assertEquals(0.2, (Double) result.getValue("avg_time"), 0.001); // 600ms / 3 = 200ms = 0.2s average
   }
 
   @Test
   public void testDifferentUnits() throws Exception {
     List<Row> rows = Arrays.asList(
       createRow("1024KB", "1s"),
       createRow("1MB", "1000ms")
     );
 
     SizeTimeAggregator directive = new SizeTimeAggregator();
     
     // Create UsageDefinition
     UsageDefinition.Builder builder = UsageDefinition.builder("size-time-aggregator");
     builder.define("size-column", TokenType.COLUMN_NAME);
     builder.define("time-column", TokenType.COLUMN_NAME);
     builder.define("target-size-column", TokenType.COLUMN_NAME);
     builder.define("target-time-column", TokenType.COLUMN_NAME);
     builder.define("size-unit", TokenType.TEXT);
     builder.define("time-unit", TokenType.TEXT);
     
     // Create TokenGroup
     TokenGroup tokenGroup = new TokenGroup(new SourceInfo(1, 1, "test"));
     tokenGroup.add(new ColumnName("size"));
     tokenGroup.add(new ColumnName("time"));
     tokenGroup.add(new ColumnName("total_size"));
     tokenGroup.add(new ColumnName("total_time"));
     tokenGroup.add(new Text("KB"));
     tokenGroup.add(new Text("ms"));
     
     // Create MapArguments
     MapArguments args = new MapArguments(builder.build(), tokenGroup);
     directive.initialize(args);
 
     List<Row> results = directive.execute(rows, null);
     Assert.assertEquals(1, results.size());
     
     Row result = results.get(0);
     Assert.assertEquals(2048.0, (Double) result.getValue("total_size"), 0.001); // 2048KB total
     Assert.assertEquals(2000.0, (Double) result.getValue("total_time"), 0.001); // 2000ms total
   }
 
   private Row createRow(String size, String time) {
     Row row = new Row();
     row.add("size", size);
     row.add("time", time);
     return row;
   }
 } 