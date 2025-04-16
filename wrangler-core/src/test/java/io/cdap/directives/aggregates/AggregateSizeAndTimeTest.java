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

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.proto.Contexts;

/**
 * Tests {@link AggregateSizeAndTime}
 */
public class AggregateSizeAndTimeTest {

  @Test
  public void testAggregateSizeAndTime() throws Exception {
    // Create test data with various byte sizes and time durations
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "1MB").add("response_time", "1s"));
    rows.add(new Row("data_transfer_size", "2MB").add("response_time", "2s"));
    rows.add(new Row("data_transfer_size", "3MB").add("response_time", "3s"));
    rows.add(new Row("data_transfer_size", "4MB").add("response_time", "4s"));
    rows.add(new Row("data_transfer_size", "5MB").add("response_time", "5s"));

    // Define the recipe for aggregation
    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    // Execute the recipe
    List<Row> results = TestingRig.execute(recipe, rows, new ExecutorContext() {
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
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset(TransientVariableScope scope) {}

          @Override
          public <T> T get(String name) {
            return null;
          }

          @Override
          public void set(TransientVariableScope scope, String name, Object value) {}

          @Override
          public void increment(TransientVariableScope scope, String name, long value) {}

          @Override
          public Set<String> getVariables() {
            return null;
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });

    // Verify the results
    Assert.assertEquals(1, results.size());
    
    // Expected total size: 1MB + 2MB + 3MB + 4MB + 5MB = 15MB
    Assert.assertEquals(15.0, ((Number)results.get(0).getValue("total_size_mb")).doubleValue(), 0.001);
    
    // Expected total time: 1s + 2s + 3s + 4s + 5s = 15s
    Assert.assertEquals(15.0, ((Number)results.get(0).getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test
  public void testAggregateWithDifferentUnits() throws Exception {
    // Create test data with different units
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "1024KB").add("response_time", "1000ms"));
    rows.add(new Row("data_transfer_size", "1GB").add("response_time", "1m"));
    rows.add(new Row("data_transfer_size", "1TB").add("response_time", "1h"));

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows, new ExecutorContext() {
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
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset(TransientVariableScope scope) {}

          @Override
          public <T> T get(String name) {
            return null;
          }

          @Override
          public void set(TransientVariableScope scope, String name, Object value) {}

          @Override
          public void increment(TransientVariableScope scope, String name, long value) {}

          @Override
          public Set<String> getVariables() {
            return null;
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });

    Assert.assertEquals(1, results.size());
    
    // Expected total size: 1MB + 1024MB + 1048576MB = 1049601MB
    Assert.assertEquals(1049601.0, ((Number)results.get(0).getValue("total_size_mb")).doubleValue(), 0.001);
    
    // Expected total time: 1s + 60s + 3600s = 3661s
    Assert.assertEquals(3661.0, ((Number)results.get(0).getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test
  public void testAggregateWithZeroValues() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "0MB").add("response_time", "0s"));
    rows.add(new Row("data_transfer_size", "0KB").add("response_time", "0ms"));
    rows.add(new Row("data_transfer_size", "0GB").add("response_time", "0m"));

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows, new ExecutorContext() {
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
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset(TransientVariableScope scope) {}

          @Override
          public <T> T get(String name) {
            return null;
          }

          @Override
          public void set(TransientVariableScope scope, String name, Object value) {}

          @Override
          public void increment(TransientVariableScope scope, String name, long value) {}

          @Override
          public Set<String> getVariables() {
            return null;
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });

    Assert.assertEquals(1, results.size());
    Assert.assertEquals(0.0, ((Number)results.get(0).getValue("total_size_mb")).doubleValue(), 0.001);
    Assert.assertEquals(0.0, ((Number)results.get(0).getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test
  public void testAggregateWithLargeNumbers() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "1000TB").add("response_time", "1000h"));
    rows.add(new Row("data_transfer_size", "1000PB").add("response_time", "1000d"));

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows, new ExecutorContext() {
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
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset(TransientVariableScope scope) {}

          @Override
          public <T> T get(String name) {
            return null;
          }

          @Override
          public void set(TransientVariableScope scope, String name, Object value) {}

          @Override
          public void increment(TransientVariableScope scope, String name, long value) {}

          @Override
          public Set<String> getVariables() {
            return null;
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });

    Assert.assertEquals(1, results.size());
    // 1000TB = 1000 * 1024 * 1024 MB
    // 1000PB = 1000 * 1024 * 1024 * 1024 MB
    double expectedSize = (1000.0 * 1024 * 1024) + (1000.0 * 1024 * 1024 * 1024);
    Assert.assertEquals(expectedSize, ((Number)results.get(0).getValue("total_size_mb")).doubleValue(), 0.001);
    
    // 1000h = 1000 * 3600 seconds
    // 1000d = 1000 * 24 * 3600 seconds
    double expectedTime = (1000.0 * 3600) + (1000.0 * 24 * 3600);
    Assert.assertEquals(expectedTime, ((Number)results.get(0).getValue("total_time_sec")).doubleValue(), 0.001);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSizeFormat() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "invalid").add("response_time", "1s"));

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    TestingRig.execute(recipe, rows, new ExecutorContext() {
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
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset(TransientVariableScope scope) {}

          @Override
          public <T> T get(String name) {
            return null;
          }

          @Override
          public void set(TransientVariableScope scope, String name, Object value) {}

          @Override
          public void increment(TransientVariableScope scope, String name, long value) {}

          @Override
          public Set<String> getVariables() {
            return null;
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeFormat() throws Exception {
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("data_transfer_size", "1MB").add("response_time", "invalid"));

    String[] recipe = new String[]{
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    TestingRig.execute(recipe, rows, new ExecutorContext() {
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
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset(TransientVariableScope scope) {}

          @Override
          public <T> T get(String name) {
            return null;
          }

          @Override
          public void set(TransientVariableScope scope, String name, Object value) {}

          @Override
          public void increment(TransientVariableScope scope, String name, long value) {}

          @Override
          public Set<String> getVariables() {
            return null;
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });
  }

  @Test
  public void testMBToBytesConversion() throws Exception {
    // Create test data with MB values
    List<Row> rows = new ArrayList<>();
    rows.add(new Row("size", "1MB"));
    rows.add(new Row("size", "2MB"));
    rows.add(new Row("size", "0.5MB"));

    // Define recipe to aggregate sizes
    String[] recipe = new String[]{
      "aggregate-size-and-time total_size size total_time time"
    };

    // Execute the recipe
    rows = TestingRig.execute(recipe, rows);

    // Verify the results
    // 1MB = 1,048,576 bytes
    // 2MB = 2,097,152 bytes
    // 0.5MB = 524,288 bytes
    // Total = 3,670,016 bytes
    Assert.assertEquals(3, rows.size());
    Assert.assertEquals(3670016.0, (double) rows.get(0).getValue("total_size"), 0.001);
  }
} 