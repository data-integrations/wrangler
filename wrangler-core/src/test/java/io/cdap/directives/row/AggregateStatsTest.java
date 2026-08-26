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
package io.cdap.directives.row;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.TestingRig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

    @Test
    public void testAggregation() throws Exception {
        String[] recipe = {
                "aggregate-stats :data_transfer :duration total_size_mb total_time_sec"
        };

        List<Row> rows = Arrays.asList(
                new Row("data_transfer", "1.5MB").add("duration", "2s"),
                new Row("data_transfer", "512KB").add("duration", "3s")
        );

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Row result = results.get(0);
        Assert.assertTrue(result.getValue("total_size_mb") instanceof Long);
        Assert.assertTrue(result.getValue("total_time_sec") instanceof Long);

        long expectedSize = (long)(1.5 * 1024 * 1024 + 512 * 1024);
        long expectedDuration = 2 + 3;

        Assert.assertEquals(expectedSize, result.getValue("total_size_mb"));
        Assert.assertEquals(expectedDuration, result.getValue("total_time_sec"));
    }
}
