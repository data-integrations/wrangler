/*
 * Copyright © 2017-2019 Cask Data, Inc.
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

import io.cdap.directives.AggregateStats;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

  @Test
  public void testAggregation() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("data_transfer", "1MB").add("response_time", "1.5s"),
      new Row("data_transfer", "512KB").add("response_time", "500ms")
    );

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());

    Row result = results.get(0);
    double totalSizeMB = (1 * 1024 * 1024 + 512 * 1024) / (1024.0 * 1024); // 1.5 MB
    double totalTimeSec = (1500 + 500) / 1000.0; // 2.0 seconds

    Assert.assertEquals(totalSizeMB, (double) result.getValue("total_size_mb"), 0.001);
    Assert.assertEquals(totalTimeSec, (double) result.getValue("total_time_sec"), 0.001);
  }
}

