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

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

public class ByteAndTimeConversionTest {

    @Test
    public void testAggregateStatsTotalMode() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("data_transfer_size", "1MB").add("response_time", "1s"));
        rows.add(new Row("data_transfer_size", "1.5MB").add("response_time", "2s"));
        rows.add(new Row("data_transfer_size", "512KB").add("response_time", "500ms"));

        String[] recipe = new String[] {
                "#pragma version 2.0;",
                "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec;"
        };

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(3, results.size());

        System.out.println("<----------------------results---------------------->");

        System.out.println(results);

        double expectedTotalSizeMB = (3);
        double expectedTotalTimeSeconds = (3.5);

        Row result = results.get(0);

        double actualSizeMB = Double
                .parseDouble(result.getValue("total_size_mb").toString().replaceAll("[^\\d.]+", ""));
        double actualTimeSec = Double
                .parseDouble(result.getValue("total_time_sec").toString().replaceAll("[^\\d.]+", ""));

        Assert.assertEquals(expectedTotalSizeMB, actualSizeMB, 0.0);
        Assert.assertEquals(expectedTotalTimeSeconds, actualTimeSec, 0.0);

    }

    @Test
    public void testAggregateStatsAverageMode() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("data_transfer_size", "1MB").add("response_time", "1s"));
        rows.add(new Row("data_transfer_size", "1MB").add("response_time", "1s"));

        String[] recipe = new String[] {
                "aggregate-stats :data_transfer_size :response_time :total_size_mb :total_time_sec"
        };

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(2, results.size());

        double expectedAvgSizeMB = (2) / 2.0;
        double expectedAvgTimeSec = (2) / 2.0;

        Row result = results.get(0);
        double actualSizeMB = Double
                .parseDouble(result.getValue("total_size_mb").toString().replaceAll("[^\\d.]+", ""));
        double actualTimeSec = Double
                .parseDouble(result.getValue("total_time_sec").toString().replaceAll("[^\\d.]+", ""));
        Assert.assertEquals(expectedAvgSizeMB, actualSizeMB, 0.001);
        Assert.assertEquals(expectedAvgTimeSec, actualTimeSec, 0.001);

    }

}
