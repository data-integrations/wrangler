/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.cdap.directives.aggregates;

import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.ExecutorContext.Environment;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.proto.Contexts;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests {@link AggregateStats}.
 */
public class AggregateStatsTest {

  static {
    try {
      Class.forName("io.cdap.directives.aggregates.AggregateStats");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load AggregateStats directive", e);
    }
  }

  @Test
  public void testAggregateStatsDirective() throws Exception {
    List<Row> inputRows = Arrays.asList(
      new Row("data_size", "10KB").add("duration", "500ms"),
      new Row("data_size", "2MB").add("duration", "1.5s"),
      new Row("data_size", "512KB").add("duration", "250ms")
    );

    String[] recipe = new String[] {
      "aggregate-stats :data_size :duration total_size_mb total_time_sec"
    };

    ExecutorContext context = new ExecutorContext() {
      @Override
      public Environment getEnvironment() {
        return Environment.TESTING;
      }

      @Override
      public String getNamespace() {
        return Contexts.SYSTEM;
      }

      @Override
      public StageMetrics getMetrics() {
        return null;
      }

      @Override
      public String getContextName() {
        return "test";
      }

      @Override
      public Map<String, String> getProperties() {
        return new HashMap<>();
      }

      @Override
      public URL getService(final String applicationId, final String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return null;
      }

      @Override
      public <T> Lookup<T> provide(final String name, final Map<String, String> map) {
        return null;
      }
    };

    List<Row> result = TestingRig.execute(recipe, inputRows, context);

    Assert.assertEquals(1, result.size());
    Row output = result.get(0);

    long totalBytes = 10 * 1024 + 2 * 1024 * 1024 + 512 * 1024;
    double expectedSizeMb = totalBytes / (1024.0 * 1024.0);

    long totalMillis = 500 + 1500 + 250;
    double expectedTimeSec = totalMillis / 1000.0;

    Assert.assertEquals(expectedSizeMb,
                        (double) output.getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTimeSec,
                        (double) output.getValue("total_time_sec"), 0.001);
  }
}