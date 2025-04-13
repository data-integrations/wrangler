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
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.directives.aggregates.AggregateStats;
import io.cdap.directives.aggregates.DefaultTransientStore;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.executor.RecipePipelineExecutor;
import io.cdap.wrangler.proto.Contexts;
import io.cdap.wrangler.registry.CompositeDirectiveRegistry;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.DirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Tests for AggregateStats directive.
 */
public class AggregateStatsTest {

    private static class TestContext implements ExecutorContext {
        private final TransientStore store;

        TestContext(TransientStore store) {
            this.store = store;
        }

        @Override
        public TransientStore getTransientStore() {
            return store;
        }

        @Override
        public ExecutorContext.Environment getEnvironment() {
            return ExecutorContext.Environment.TESTING;
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
            return "test-context";
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
        public <T> Lookup<T> provide(String table, Map<String, String> context) {
            return null;
        }
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
            return null;
        }
    }

    @Test
    public void testTotalAggregation() throws Exception {
        String[] recipe = {
                "aggregate-stats :size :time :total_size_mb :total_time_s mb s total"
        };
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("size", 10_240L).add("time", 5_000_000L)); // 10KB, 5ms
        rows.add(new Row("size", 5_242_880L).add("time", 2_000_000_000L)); // 5MB, 2s

        double expectedSizeMB = (10_240.0 + 5_242_880.0) / (1024 * 1024.0);
        double expectedTimeS = (5_000_000.0 + 2_000_000_000.0) / 1_000_000_000.0;

        DefaultTransientStore store = new DefaultTransientStore();
        ExecutorContext context = new TestContext(store);
        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(new TestDirectiveRegistry());
        String migrate = new MigrateToV2(recipe).migrate();
        System.out.println("Migrated recipe: " + migrate);
        RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
        List<Directive> directives = parser.parse();
        System.out.println("Parsed directives: " + directives);
        RecipePipeline pipeline = new RecipePipelineExecutor(parser, context);
        try {
            pipeline.execute(rows);
            System.out.println("Pipeline executed");
        } catch (Exception e) {
            System.out.println("Execution failed: " + e.getMessage());
            throw e;
        }

        Row output = (Row) store.get("aggregate_stats_output_row");
        System.out.println("Output row: " + output);
        Assert.assertNotNull("Output row should exist", output);
        Assert.assertEquals(expectedSizeMB, ((Number) output.getValue("total_size_mb")).doubleValue(), 0.001);
        Assert.assertEquals(expectedTimeS, ((Number) output.getValue("total_time_s")).doubleValue(), 0.001);
    }

    @Test
    public void testAverageAggregation() throws Exception {
        String[] recipe = {
                "aggregate-stats :size :time :avg_size_mb :avg_time_s mb s average"
        };
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("size", 10_240L).add("time", 5_000_000L));
        rows.add(new Row("size", 5_242_880L).add("time", 2_000_000_000L));

        double expectedSizeMB = (10_240.0 + 5_242_880.0) / (2 * 1024 * 1024.0);
        double expectedTimeS = (5_000_000.0 + 2_000_000_000.0) / (2 * 1_000_000_000.0);

        DefaultTransientStore store = new DefaultTransientStore();
        ExecutorContext context = new TestContext(store);
        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(new TestDirectiveRegistry());
        String migrate = new MigrateToV2(recipe).migrate();
        System.out.println("Migrated recipe: " + migrate);
        RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
        List<Directive> directives = parser.parse();
        System.out.println("Parsed directives: " + directives);
        RecipePipeline pipeline = new RecipePipelineExecutor(parser, context);
        try {
            pipeline.execute(rows);
            System.out.println("Pipeline executed");
        } catch (Exception e) {
            System.out.println("Execution failed: " + e.getMessage());
            throw e;
        }

        Row output = (Row) store.get("aggregate_stats_output_row");
        System.out.println("Output row: " + output);
        Assert.assertNotNull("Output row should exist", output);
        Assert.assertEquals(expectedSizeMB, ((Number) output.getValue("avg_size_mb")).doubleValue(), 0.001);
        Assert.assertEquals(expectedTimeS, ((Number) output.getValue("avg_time_s")).doubleValue(), 0.001);
    }

    @Test
    public void testNullValues() throws Exception {
        String[] recipe = {
                "aggregate-stats :size :time :total_size_mb :total_time_s mb s total"
        };
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("size", 10_240L).add("time", 5_000_000L));
        rows.add(new Row("size", null).add("time", null));

        double expectedSizeMB = 10_240.0 / (1024 * 1024.0);
        double expectedTimeS = 5_000_000.0 / 1_000_000_000.0;

        DefaultTransientStore store = new DefaultTransientStore();
        ExecutorContext context = new TestContext(store);
        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(new TestDirectiveRegistry());
        String migrate = new MigrateToV2(recipe).migrate();
        System.out.println("Migrated recipe: " + migrate);
        RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
        List<Directive> directives = parser.parse();
        System.out.println("Parsed directives: " + directives);
        RecipePipeline pipeline = new RecipePipelineExecutor(parser, context);
        try {
            pipeline.execute(rows);
            System.out.println("Pipeline executed");
        } catch (Exception e) {
            System.out.println("Execution failed: " + e.getMessage());
            throw e;
        }

        Row output = (Row) store.get("aggregate_stats_output_row");
        System.out.println("Output row: " + output);
        Assert.assertNotNull("Output row should exist", output);
        Assert.assertEquals(expectedSizeMB, ((Number) output.getValue("total_size_mb")).doubleValue(), 0.001);
        Assert.assertEquals(expectedTimeS, ((Number) output.getValue("total_time_s")).doubleValue(), 0.001);
    }

    @Test
    public void testEmptyInput() throws Exception {
        String[] recipe = {
                "aggregate-stats :size :time :total_size_mb :total_time_s mb s total"
        };
        List<Row> rows = new ArrayList<>();

        DefaultTransientStore store = new DefaultTransientStore();
        ExecutorContext context = new TestContext(store);
        CompositeDirectiveRegistry registry = new CompositeDirectiveRegistry(new TestDirectiveRegistry());
        String migrate = new MigrateToV2(recipe).migrate();
        System.out.println("Migrated recipe: " + migrate);
        RecipeParser parser = new GrammarBasedParser(Contexts.SYSTEM, migrate, registry);
        List<Directive> directives = parser.parse();
        System.out.println("Parsed directives: " + directives);
        RecipePipeline pipeline = new RecipePipelineExecutor(parser, context);
        try {
            pipeline.execute(rows);
            System.out.println("Pipeline executed");
        } catch (Exception e) {
            System.out.println("Execution failed: " + e.getMessage());
            throw e;
        }

        Row output = (Row) store.get("aggregate_stats_output_row");
        System.out.println("Output row: " + output);
        Assert.assertNull("Output row should not exist for empty input", output);
    }
}