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

package io.cdap.wrangler.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.RecipeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests to validate grammar and parsing of the aggregate_size_duration
 * directive.
 */
public class AggregateSizeDurationParserTest {

    @Test
    public void testValidAggregateSizeDurationDirective() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("size", "1MB").add("time", "2s"));
        rows.add(new Row().add("size", "512KB").add("time", "500ms"));

        String[] recipe = new String[] {
                "aggregate_size_duration size time totalSize totalTime MB s"
        };

        List<Row> results = TestingRig.execute(recipe, rows);

        // Only one aggregated row expected
        Assert.assertEquals(1, results.size());

        Row result = results.get(0);
        // (1MB + 512KB = 1.5MB), (2s + 500ms = 2.5s)
        Assert.assertEquals(1.5, (Double) result.getValue("totalSize"), 0.001);
        Assert.assertEquals(2.5, (Double) result.getValue("totalTime"), 0.001);
    }

    @Test(expected = RecipeException.class)
    public void testInvalidDirectiveTooFewArguments() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("size", "1MB").add("time", "2s"));

        String[] recipe = new String[] {
                // Missing output column names
                "aggregate_size_duration size time"
        };

        // Should throw RecipeException due to incorrect argument count
        TestingRig.execute(recipe, rows);
    }

    @Test(expected = RecipeException.class)
    public void testInvalidDirectiveUnknownUnit() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("size", "1MB").add("time", "2s"));

        String[] recipe = new String[] {
                "aggregate_size_duration size time totalSize totalTime XYZ ABC"
        };

        // Should throw due to unsupported units
        TestingRig.execute(recipe, rows);
    }

    @Test
    public void testAverageAggregation() throws Exception {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row().add("size", "2MB").add("time", "2s"));
        rows.add(new Row().add("size", "4MB").add("time", "4s"));

        String[] recipe = new String[] {
                "aggregate_size_duration size time avgSize avgTime MB s average"
        };

        List<Row> results = TestingRig.execute(recipe, rows);

        Assert.assertEquals(1, results.size());
        Row result = results.get(0);

        // (2MB + 4MB) / 2 = 3MB, (2s + 4s) / 2 = 3s
        Assert.assertEquals(3.0, (Double) result.getValue("avgSize"), 0.001);
        Assert.assertEquals(3.0, (Double) result.getValue("avgTime"), 0.001);
    }
}
