/*
 *  Copyright © 2017-2019 Cask Data, Inc.
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

package io.cdap.wrangler.registry;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.proto.Contexts;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Tests {@link CompositeDirectiveRegistry}
 */
public class CompositeDirectiveRegistryTest {

  @Plugin(type = Directive.TYPE)
  @Name("my-test")
  @Description("Test")
  public static final class MyTest implements Directive {
    private String column;

    @Override
    public List<Row> execute(List<Row> row, ExecutorContext context) {
      return row;
    }

    @Override
    public UsageDefinition define() {
      UsageDefinition.Builder builder = UsageDefinition.builder("my-test");
      builder.define("column", TokenType.COLUMN_NAME);
      return builder.build();
    }

    @Override
    public void initialize(Arguments args) {
      column = ((ColumnName) args.value("column")).value();
    }

    @Override
    public void destroy() {
      // no-op
    }
  }

  private class TestDirectiveRegistry implements DirectiveRegistry {
    private Map<String, DirectiveInfo> registry = new HashMap<>();

    public TestDirectiveRegistry() throws InstantiationException, IllegalAccessException {
      registry.put("my-test", DirectiveInfo.fromUser(MyTest.class,
                                                     new ArtifactId("dummy", new ArtifactVersion("1.0"),
                                                                    ArtifactScope.USER)));
    }

    @Override
    public Iterable<DirectiveInfo> list(String namespace) {
      return registry.values();
    }

    @Nullable
    @Override
    public DirectiveInfo get(String namespace, String name) {
      return registry.get(name);
    }

    @Override
    public void reload(String namespace) {
      // no-op
    }

    @Nullable
    @Override
    public ArtifactSummary getLatestWranglerArtifact() {
      return null;
    }

    @Override
    public void close() {
      // no-op
    }
  }

  @Test
  public void testIteratorUsage() throws Exception {
    DirectiveRegistry registry = new CompositeDirectiveRegistry(
      SystemDirectiveRegistry.INSTANCE,
      new TestDirectiveRegistry()
    );

    Iterator<DirectiveInfo> iterator = registry.list(Contexts.SYSTEM).iterator();
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    Assert.assertEquals(85, count);

    registry.reload("");

    iterator = registry.list(Contexts.SYSTEM).iterator();
    count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      count++;
    }
    Assert.assertEquals(85, count);

  }
}
