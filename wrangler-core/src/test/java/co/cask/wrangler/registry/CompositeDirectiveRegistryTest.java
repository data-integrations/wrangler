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

package co.cask.wrangler.registry;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.wrangler.api.Arguments;
import co.cask.wrangler.api.Directive;
import co.cask.wrangler.api.DirectiveExecutionException;
import co.cask.wrangler.api.DirectiveInfo;
import co.cask.wrangler.api.DirectiveLoadException;
import co.cask.wrangler.api.DirectiveParseException;
import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.ErrorRowException;
import co.cask.wrangler.api.ExecutorContext;
import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.parser.ColumnName;
import co.cask.wrangler.api.parser.TokenType;
import co.cask.wrangler.api.parser.UsageDefinition;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Tests {@link CompositeDirectiveRegistry}
 */
public class CompositeDirectiveRegistryTest {

  @Plugin(type = Directive.Type)
  @Name("my-test")
  @Description("Test")
  public static final class MyTest implements Directive {
    private String column;

    @Override
    public List<Row> execute(List<Row> row, ExecutorContext context) throws DirectiveExecutionException, ErrorRowException {
      return row;
    }

    @Override
    public UsageDefinition define() {
      UsageDefinition.Builder builder = UsageDefinition.builder("my-test");
      builder.define("column", TokenType.COLUMN_NAME);
      return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
      column = ((ColumnName) args.value("column")).value();
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  private class TestDirectiveRegistry implements DirectiveRegistry {
    private Map<String, DirectiveInfo> registry = new HashMap<>();

    public TestDirectiveRegistry() {
      try {
        registry.put("my-test", new DirectiveInfo(DirectiveInfo.Scope.USER, MyTest.class));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      } catch (InstantiationException e) {
        e.printStackTrace();
      }
    }

    @Nullable
    @Override
    public DirectiveInfo get(String name) throws DirectiveLoadException {
      return registry.get(name);
    }

    @Override
    public void reload() throws DirectiveLoadException {
      // no-op
    }

    @Override
    public JsonElement toJson() {
      return new JsonObject();
    }

    @Override
    public Iterator<DirectiveInfo> iterator() {
      return registry.values().iterator();
    }

    @Override
    public void close() throws IOException {
      // no-op
    }
  }
  @Test
  public void testIteratorUsage() throws Exception {
    DirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry(),
      new TestDirectiveRegistry()
    );

    Iterator<DirectiveInfo> iterator = registry.iterator();
    int count = 0;
    while(iterator.hasNext()) {
      iterator.next();
      count++;
    }
    Assert.assertEquals(73, count);

    registry.reload();

    iterator = registry.iterator();
    count = 0;
    while(iterator.hasNext()) {
      iterator.next();
      count++;
    }
    Assert.assertEquals(73, count);

  }
}
