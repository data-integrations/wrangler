/*
 * Copyright © 2025 Cask Data, Inc.
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
package io.cdap.wrangler.directive;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.executor.ExecutorContext;
import io.cdap.wrangler.api.parser.DirectiveLoadException;
import io.cdap.wrangler.api.parser.DirectiveExecutionException;
import io.cdap.wrangler.directive.AggregateDirective;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import io.cdap.wrangler.api.parser.Arguments;
import java.util.HashMap;
import java.util.Map;

public class AggregateDirectiveTest {
    private AggregateDirective directive;
    private ExecutorContext context;

    @Before
    public void setUp() throws DirectiveLoadException {
        directive = new AggregateDirective();
        directive.initialize(new MockArguments(
            "sourceSizeColumn", ":size",
            "sourceTimeColumn", ":time",
            "targetSizeColumn", ":totalSize",
            "targetTimeColumn", ":totalTime",
            "sizeUnit", "MB",
            "timeUnit", "s",
            "aggregationType", "total"
        ));
        context = new MockExecutorContext();
    }

    @Test
    public void testAggregateDirective() throws DirectiveExecutionException {
        // Prepare test data
        List<Row> rows = new ArrayList<>();
        rows.add(new Row(":size", "1.5MB").add(":time", "2.5s"));
        rows.add(new Row(":size", "2MB").add(":time", "1.5s"));

        // Execute the directive
        directive.execute(rows, context);
        List<Row> result = directive.finalize(rows, context);

        // Calculate expected values
        double expectedTotalSizeInMB = (1.5 + 2.0); // Sum of sizes in MB
        double expectedTotalTimeInSeconds = (2.5 + 1.5); // Sum of times in seconds

        // Assert the results
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(expectedTotalSizeInMB, result.get(0).getValue(":totalSize"), 0.001);
        Assert.assertEquals(expectedTotalTimeInSeconds, result.get(0).getValue(":totalTime"), 0.001);
    }

    @Test
    public void testAverageAggregation() throws DirectiveLoadException, DirectiveExecutionException {
        directive.initialize(new MockArguments(
            "sourceSizeColumn", ":size",
            "sourceTimeColumn", ":time",
            "targetSizeColumn", ":avgSize",
            "targetTimeColumn", ":avgTime",
            "sizeUnit", "MB",
            "timeUnit", "s",
            "aggregationType", "average"
        ));

        List<Row> rows = new ArrayList<>();
        rows.add(new Row(":size", "1.5MB").add(":time", "2.5s"));
        rows.add(new Row(":size", "2MB").add(":time", "1.5s"));

        directive.execute(rows, context);
        List<Row> result = directive.finalize(rows, context);

        assertEquals(1, result.size());
        Row aggregatedRow = result.get(0);

        assertEquals(1.75, aggregatedRow.getValue(":avgSize")); // Average size in MB
        assertEquals(2.0, aggregatedRow.getValue(":avgTime")); // Average time in seconds
    }

    @Test(expected = DirectiveExecutionException.class)
    public void testInvalidSizeUnit() throws DirectiveLoadException {
        directive.initialize(new MockArguments(
            "sourceSizeColumn", ":size",
            "sourceTimeColumn", ":time",
            "targetSizeColumn", ":totalSize",
            "targetTimeColumn", ":totalTime",
            "sizeUnit", "INVALID",
            "timeUnit", "s",
            "aggregationType", "total"
        ));
    }

    @Test(expected = DirectiveExecutionException.class)
    public void testInvalidTimeUnit() throws DirectiveLoadException {
        directive.initialize(new MockArguments(
            "sourceSizeColumn", ":size",
            "sourceTimeColumn", ":time",
            "targetSizeColumn", ":totalSize",
            "targetTimeColumn", ":totalTime",
            "sizeUnit", "MB",
            "timeUnit", "INVALID",
            "aggregationType", "total"
        ));
    }

    // Mock classes for testing
    private static class MockArguments extends Arguments {
        private final Map<String, Object> arguments;

        public MockArguments(Object... args) {
            arguments = new HashMap<>();
            for (int i = 0; i < args.length; i += 2) {
                String key = (String) args[i];
                Object value = args[i + 1];
                arguments.put(key, value);
            }
        }

        @Override
        public Object value(String name) {
            return arguments.get(name);
        }

        @Override
        public Object valueOrDefault(String name, Object defaultValue) {
            return arguments.getOrDefault(name, defaultValue);
        }
    }

    private static class MockExecutorContext implements ExecutorContext {
        // Implement necessary methods for mock context
    }
}
