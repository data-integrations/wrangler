/*
 *  Copyright © 2017 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package co.cask.directives.aggregates;

import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.wrangler.TestingRig;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.TransientStore;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests {@link SetTransientVariable}
 */
public class SetTransientVariableTest {

  @Test
  public void testSettingOfVariable() throws Exception {
    String[] recipe = new String[] {
      "set-variable test fwd == 0 ? A : test"
    };

    List<Row> rows = new ArrayList<>();
    rows.add(new Row("fwd", 0).add("A", 2));
    rows.add(new Row("fwd", 1).add("A", 2));

    final Map<String, Object> s = new HashMap<>();
    rows = TestingRig.execute(recipe, rows, new ExecutorContext() {
      @Override
      public Environment getEnvironment() {
        return Environment.TESTING;
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
        return null;
      }

      @Override
      public URL getService(String applicationId, String serviceId) {
        return null;
      }

      @Override
      public TransientStore getTransientStore() {
        return new TransientStore() {
          @Override
          public void reset() {

          }

          @Override
          public <T> T get(String name) {
            return (T) s.get(name);
          }

          @Override
          public void set(String name, Object value) {
            s.put(name, value);
          }

          @Override
          public void increment(String name, long value) {

          }

          @Override
          public Set<String> getVariables() {
            return s.keySet();
          }
        };
      }

      @Override
      public <T> Lookup<T> provide(String s, Map<String, String> map) {
        return null;
      }
    });
    Assert.assertEquals(2, rows.size());
    Assert.assertEquals(2, s.get("test"));
  }
}