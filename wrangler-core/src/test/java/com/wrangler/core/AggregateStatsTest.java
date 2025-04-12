/*
 * Copyright 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cdap.directives.aggregates;
import io.cdap.directives.aggregates.AggregateStats;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AggregateStatsTest {

    @Test
    public void testSumAggregation() {
        AggregateStats aggregateStats = new AggregateStats("sum");
        assertEquals(10240, aggregateStats.aggregate(new long[]{1024, 2048, 4096, 5120}));  // Sum of byte sizes in bytes
    }

    @Test
    public void testAverageAggregation() {
        AggregateStats aggregateStats = new AggregateStats("avg");
        assertEquals(2048, aggregateStats.aggregate(new long[]{1024, 2048, 4096, 5120}));  // Average byte size in bytes
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAggregationDirective() {
        AggregateStats aggregateStats = new AggregateStats("invalid");
        aggregateStats.aggregate(new long[]{1024, 2048});
    }
}
