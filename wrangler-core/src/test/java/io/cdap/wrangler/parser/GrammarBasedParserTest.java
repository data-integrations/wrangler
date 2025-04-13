/*
 * Copyright © 2017-2025 Cask Data, Inc.
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

package io.cdap.wrangler.parser;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.directives.aggregates.AggregateStats;
import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.CompileStatus;
import io.cdap.wrangler.api.Compiler;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.RecipeParser;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Tests {@link GrammarBasedParser}
 */
public class GrammarBasedParserTest {

  @Test
  public void testBasic() throws Exception {
    String[] recipe = new String[] {
            "#pragma version 2.0;",
            "rename :col1 :col2",
            "parse-as-csv :body ',' true;",
            "#pragma load-directives text-reverse, text-exchange;",
            "${macro} ${macro_2}",
            "${macro_${test}}"
    };
    RecipeParser parser = TestingRig.parse(recipe);
    List<Directive> directives = parser.parse();
    Assert.assertEquals(2, directives.size());
  }

  @Test
  public void testLoadableDirectives() throws Exception {
    String[] recipe = new String[] {
            "#pragma version 2.0;",
            "#pragma load-directives text-reverse, text-exchange;",
            "rename col1 col2",
            "parse-as-csv body , true",
            "text-reverse :body;",
            "test prop: { a='b', b=1.0, c=true};",
            "#pragma load-directives test-change,text-exchange, test1,test2,test3,test4;"
    };
    Compiler compiler = new RecipeCompiler();
    CompileStatus status = compiler.compile(String.join("\n", recipe));
    Assert.assertEquals(7, status.getSymbols().getLoadableDirectives().size());
  }

  @Test
  public void testCommentOnlyRecipe() throws Exception {
    String[] recipe = new String[] {
            "// test"
    };
    RecipeParser parser = TestingRig.parse(recipe);
    List<Directive> directives = parser.parse();
    Assert.assertEquals(0, directives.size());
  }

  private static class TestDirectiveRegistry implements DirectiveRegistry {
    private final Map<String, Map<String, DirectiveInfo>> registry = new ConcurrentSkipListMap<>();

    TestDirectiveRegistry() {
      Map<String, DirectiveInfo> directives = new ConcurrentSkipListMap<>();
      try {
        directives.put("aggregate-stats", DirectiveInfo.fromUser(AggregateStats.class, null));
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Failed to register AggregateStats", e);
      }
      registry.put(Contexts.SYSTEM, directives);
    }

    @Override
    public DirectiveInfo get(String namespace, String name) {
      return registry.getOrDefault(namespace, Collections.emptyMap()).get(name);
    }

    @Override
    public void reload(String namespace) {
      // No-op for test
    }

    @Override
    public Iterable<DirectiveInfo> list(String namespace) {
      return registry.getOrDefault(namespace, Collections.emptyMap()).values();
    }

    @Override
    public void close() {
      // No-op
    }

    @Override
    public ArtifactSummary getLatestWranglerArtifact() {
      return null; // Tests don't need artifact
    }
  }

  @Test
  public void testAggregateStatsParsing() throws Exception {
    String[] recipe = {
            "aggregate-stats :size :time :total_size_mb :total_time_s mb s total"
    };

    // Create test registry
    DirectiveRegistry testRegistry = new TestDirectiveRegistry();
    CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(testRegistry);

    // Parse recipe
    String migrate = new MigrateToV2(recipe).migrate();
    RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
    List<Directive> directives = parser.parse();

    // Verify parsing
    Assert.assertNotNull("Parsed directives should not be null", directives);
    Assert.assertFalse("Parsed directives should not be empty", directives.isEmpty());
    Assert.assertEquals("Should parse one directive", 1, directives.size());
    Assert.assertTrue("Directive should be AggregateStats", directives.get(0) instanceof AggregateStats);
  }
}