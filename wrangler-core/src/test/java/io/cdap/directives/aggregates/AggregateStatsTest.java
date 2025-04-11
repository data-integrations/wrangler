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
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */


package io.cdap.directives.aggregates;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggregateStatsTest {

    @Test
    public void testAggregateStatsTotalInMbAndSeconds() throws Exception {
        List<Row> rows = Arrays.asList(
                new Row("data_transfer_size", "10KB").add("response_time", "100ms"),
                new Row("data_transfer_size", "5MB").add("response_time", "1.5s"),
                new Row("data_transfer_size", "1024").add("response_time", "250ms") // 1024 bytes, 250ms
        );

        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
        };

        List<Row> result = TestingRig.execute(recipe, rows);

        // Calculate expected totals
        double expectedTotalBytes = (10 * 1024) + (5 * 1024 * 1024) + 1024;
        double expectedMB = expectedTotalBytes / (1024 * 1024);

        double expectedNs = (100e6) + (1.5e9) + (250e6);
        double expectedSeconds = expectedNs / 1e9;

        Assert.assertEquals(1, result.size());
        Row output = result.get(0);

        Assert.assertEquals(expectedMB, (double) output.getValue("total_size_mb"), 0.001);
        Assert.assertEquals(expectedSeconds, (double) output.getValue("total_time_sec"), 0.001);
    }
}
