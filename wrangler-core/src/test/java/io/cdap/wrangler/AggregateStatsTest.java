/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package io.cdap.wrangler;

 import com.google.gson.JsonElement;
 import io.cdap.directives.aggregates.AggregateStats;
 import io.cdap.wrangler.api.Arguments;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.parser.ColumnName;
 import io.cdap.wrangler.api.parser.Text;
 import io.cdap.wrangler.api.parser.Token;
 import io.cdap.wrangler.api.parser.TokenType;
 import org.junit.Assert;
 import org.junit.Test;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.Random;
 
 public class AggregateStatsTest {
 
   @Test
   public void testTotalAggregation() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "2s"));
     rows.add(new Row().add("data_transfer_size", "5KB").add("response_time", "500ms"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
 
     Row output = result.get(0);
     double expectedMB = 15360.0 / (1024 * 1024);
     double expectedSeconds = 2.5;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSeconds, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testAverageAggregation() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "2s"));
     rows.add(new Row().add("data_transfer_size", "6KB").add("response_time", "1s"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("average");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
 
     Row output = result.get(0);
     double expectedMB = ((10240.0 + 6144.0) / 2) / (1024 * 1024);
     double expectedSeconds = 1.5;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSeconds, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testMinAggregation() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "2s"));
     rows.add(new Row().add("data_transfer_size", "6KB").add("response_time", "1s"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("min");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
 
     Row output = result.get(0);
     double expectedMB = 6144.0 / (1024 * 1024);
     double expectedSeconds = 1.0;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSeconds, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testMaxAggregation() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "10KB").add("response_time", "2s"));
     rows.add(new Row().add("data_transfer_size", "6KB").add("response_time", "1s"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("max");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
 
     Row output = result.get(0);
     double expectedMB = 10240.0 / (1024 * 1024);
     double expectedSeconds = 2.0;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSeconds, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testEmptyInputRows() throws Exception {
     List<Row> rows = new ArrayList<>();
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(0, result.size());
   }
 
   @Test
   public void testInvalidData() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "invalidKB").add("response_time", "invalidTime"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     try {
       directive.execute(rows, null);
       Assert.fail("Expected an exception for invalid data");
     } catch (Exception e) {
       Assert.assertTrue(e.getMessage().contains("Error aggregating stats"));
     }
   }
 
   @Test
   public void testLargeScaleAggregation() throws Exception {
     List<Row> rows = new ArrayList<>();
     Random random = new Random(42);
     long totalBytes = 0;
     double totalSeconds = 0;
     int count = 1000;
 
     for (int i = 0; i < count; i++) {
       int sizeInKB = 1 + random.nextInt(1024); // 1KB to 1024KB
       int timeInMs = 100 + random.nextInt(10000); // 100ms to 10s
 
       rows.add(new Row()
         .add("data_transfer_size", sizeInKB + "KB")
         .add("response_time", timeInMs + "ms"));
 
       totalBytes += sizeInKB * 1024L;
       totalSeconds += timeInMs / 1000.0;
     }
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
     Row output = result.get(0);
 
     double expectedMB = totalBytes / (1024.0 * 1024.0);
     double expectedTime = totalSeconds;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedTime, (Double) output.getValue("total_time_sec"), 0.001);
   }
   @Test
   public void testZeroValues() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "0KB").add("response_time", "0s"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
     Row output = result.get(0);
     Assert.assertEquals(0.0, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(0.0, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testMixedUnits() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "1MB").add("response_time", "1s"));
     rows.add(new Row().add("data_transfer_size", "1024KB").add("response_time", "1000ms"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
     Row output = result.get(0);
     Assert.assertEquals(2.0, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(2.0, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testExtremeValues() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "10GB").add("response_time", "2h"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
     Row output = result.get(0);
     Assert.assertEquals(10240.0, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(7200.0, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testNegativeValues() throws Exception {
     List<Row> rows = new ArrayList<>();
     rows.add(new Row().add("data_transfer_size", "-5KB").add("response_time", "-3s"));
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("total");
 
     directive.initialize(args);
     try {
       directive.execute(rows, null);
       Assert.fail("Expected an exception for negative values");
     } catch (Exception e) {
       Assert.assertTrue(e.getMessage().contains("Error aggregating stats"));
     }
   }
 
   @Test
   public void testLargeScaleAverage() throws Exception {
     List<Row> rows = new ArrayList<>();
     Random random = new Random(123);
     long totalBytes = 0;
     double totalSeconds = 0;
     int count = 1000;
 
     for (int i = 0; i < count; i++) {
       int sizeInKB = 1 + random.nextInt(512);
       int timeInMs = 100 + random.nextInt(5000);
 
       rows.add(new Row()
         .add("data_transfer_size", sizeInKB + "KB")
         .add("response_time", timeInMs + "ms"));
 
       totalBytes += sizeInKB * 1024L;
       totalSeconds += timeInMs / 1000.0;
     }
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("average");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
 
     Assert.assertEquals(1, result.size());
     Row output = result.get(0);
 
     double expectedMB = (totalBytes / (double) count) / (1024.0 * 1024.0);
     double expectedSec = totalSeconds / count;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSec, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testLargeScaleMin() throws Exception {
     List<Row> rows = new ArrayList<>();
     int minKB = Integer.MAX_VALUE;
     int minMs = Integer.MAX_VALUE;
     Random random = new Random(321);
 
     for (int i = 0; i < 1000; i++) {
       int sizeInKB = 1 + random.nextInt(512);
       int timeInMs = 10 + random.nextInt(5000);
 
       minKB = Math.min(minKB, sizeInKB);
       minMs = Math.min(minMs, timeInMs);
 
       rows.add(new Row()
         .add("data_transfer_size", sizeInKB + "KB")
         .add("response_time", timeInMs + "ms"));
     }
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("min");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
     Row output = result.get(0);
 
     double expectedMB = minKB * 1024.0 / (1024.0 * 1024.0);
     double expectedSec = minMs / 1000.0;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSec, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   @Test
   public void testLargeScaleMax() throws Exception {
     List<Row> rows = new ArrayList<>();
     int maxKB = Integer.MIN_VALUE;
     int maxMs = Integer.MIN_VALUE;
     Random random = new Random(111);
 
     for (int i = 0; i < 1000; i++) {
       int sizeInKB = 1 + random.nextInt(512);
       int timeInMs = 10 + random.nextInt(5000);
 
       maxKB = Math.max(maxKB, sizeInKB);
       maxMs = Math.max(maxMs, timeInMs);
 
       rows.add(new Row()
         .add("data_transfer_size", sizeInKB + "KB")
         .add("response_time", timeInMs + "ms"));
     }
 
     AggregateStats directive = new AggregateStats();
     Arguments args = createArguments("max");
 
     directive.initialize(args);
     List<Row> result = directive.execute(rows, null);
     Row output = result.get(0);
 
     double expectedMB = maxKB * 1024.0 / (1024.0 * 1024.0);
     double expectedSec = maxMs / 1000.0;
     Assert.assertEquals(expectedMB, (Double) output.getValue("total_size_mb"), 0.001);
     Assert.assertEquals(expectedSec, (Double) output.getValue("total_time_sec"), 0.001);
   }
 
   private Arguments createArguments(String aggregationType) {
     return new Arguments() {
       @SuppressWarnings("unchecked")
       @Override
       public <T extends Token> T value(String name) {
         switch (name) {
           case "byteCol":
             return (T) new ColumnName("data_transfer_size");
           case "timeCol":
             return (T) new ColumnName("response_time");
           case "outputSizeCol":
             return (T) new Text("total_size_mb");
           case "outputTimeCol":
             return (T) new Text("total_time_sec");
           case "aggregationType":
             return (T) new Text(aggregationType);
         }
         return null;
       }
 
       @Override
       public int size() {
         return 5;
       }
 
       @Override
       public boolean contains(String name) {
         return true;
       }
 
       @Override
       public TokenType type(String name) {
         return null;
       }
 
       @Override
       public int line() {
         return 1;
       }
 
       @Override
       public int column() {
         return 0;
       }
 
       @Override
       public String source() {
         return "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec aggregationType";
       }
 
       @Override
       public JsonElement toJson() {
         return null;
       }
     };
   }
 }
 