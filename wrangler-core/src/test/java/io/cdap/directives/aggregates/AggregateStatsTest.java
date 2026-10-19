/*
  * Copyright © 2025 Cask Data, Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
  * use this file except in compliance with the License. You may obtain a copy of
  * the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  * License for the specific language governing permissions and limitations under
  * the License.
  */
 
  package io.cdap.wrangler.extension.directives.row;
 
  import io.cdap.wrangler.api.Row;
  import io.cdap.wrangler.test.RecipeTester;
  import org.junit.Assert;
  import org.junit.Test;
  
  import java.util.Arrays;
  import java.util.List;
  
  public class AggregateStatsTest {
  
    @Test
    public void testAggregateTotalSizeAndTime() throws Exception {
      String[] recipe = new String[] {
        "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec sizeUnit=MB timeUnit=seconds aggregationType=total"
      };
  
      List<Row> rows = Arrays.asList(
        new Row().add("data_transfer_size", "10kb").add("response_time", "500ms"),
        new Row().add("data_transfer_size", "2048kb").add("response_time", "1.5s")
      );
  
      List<Row> results = RecipeTester.test(recipe, rows);
  
      Assert.assertEquals(1, results.size());
  
      double expectedSizeMB = (10 * 1024 + 2048 * 1024) / (1024.0 * 1024.0); // = 2.0117 MB
      double expectedTimeSec = (500_000_000L + 1_500_000_000L) / 1_000_000_000.0; // = 2.0 sec
  
      Assert.assertEquals(expectedSizeMB, ((Number) results.get(0).getValue("total_size_mb")).doubleValue(), 0.001);
      Assert.assertEquals(expectedTimeSec, ((Number) results.get(0).getValue("total_time_sec")).doubleValue(), 0.001);
    }
  
    @Test
    public void testAggregateAverageSizeAndTime() throws Exception {
      String[] recipe = new String[] {
        "aggregate-stats :data_transfer_size :response_time avg_size_mb avg_time_sec sizeUnit=MB timeUnit=seconds aggregationType=average"
      };
  
      List<Row> rows = Arrays.asList(
        new Row().add("data_transfer_size", "1MB").add("response_time", "2s"),
        new Row().add("data_transfer_size", "3MB").add("response_time", "4s")
      );
  
      List<Row> results = RecipeTester.test(recipe, rows);
  
      Assert.assertEquals(1, results.size());
  
      double expectedSizeMB = 2.0; // Average of 1MB and 3MB
      double expectedTimeSec = 3.0; // Average of 2s and 4s
  
      Assert.assertEquals(expectedSizeMB, ((Number) results.get(0).getValue("avg_size_mb")).doubleValue(), 0.001);
      Assert.assertEquals(expectedTimeSec, ((Number) results.get(0).getValue("avg_time_sec")).doubleValue(), 0.001);
    }
  }