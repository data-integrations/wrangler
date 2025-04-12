/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.utils.TestArguments;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test class for AggregateStats Directive.
 * This class verifies the aggregation of min, max, sum, avg, count for a given
 * column.
 */
public class AggregateStatsTest {

    @Test
    public void testPositiveNumbers() throws DirectiveExecutionException {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("amount", 10));
        rows.add(new Row("amount", 20));
        rows.add(new Row("amount", 30));

        AggregateStats directive = new AggregateStats();

        try {
            directive.initialize(new TestArguments("amount"));
        } catch (DirectiveParseException e) {
            Assert.fail("Initialization failed: " + e.getMessage());
        }

        List<Row> result = directive.execute(rows, null);
        Row output = result.get(0);

        Assert.assertEquals(10.0, output.getValue("min"));
        Assert.assertEquals(30.0, output.getValue("max"));
        Assert.assertEquals(60.0, output.getValue("sum"));
        Assert.assertEquals(20.0, output.getValue("avg"));
        Assert.assertEquals(3L, output.getValue("count"));
    }

    @Test
    public void testNegativeNumbers() throws DirectiveExecutionException {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("amount", -10));
        rows.add(new Row("amount", -20));
        rows.add(new Row("amount", -5));

        AggregateStats directive = new AggregateStats();
        try {
            directive.initialize(new TestArguments("amount"));
        } catch (DirectiveParseException e) {
            Assert.fail("Initialization failed: " + e.getMessage());
        }

        List<Row> result = directive.execute(rows, null);
        Row output = result.get(0);

        Assert.assertEquals(-20.0, output.getValue("min"));
        Assert.assertEquals(-5.0, output.getValue("max"));
        Assert.assertEquals(-35.0, output.getValue("sum"));
        Assert.assertEquals(-11.666666666666666, output.getValue("avg"));
        Assert.assertEquals(3L, output.getValue("count"));
    }

    @Test(expected = DirectiveExecutionException.class)
    public void testEmptyInput() throws DirectiveExecutionException {
        List<Row> rows = new ArrayList<>();

        AggregateStats directive = new AggregateStats();
        try {
            directive.initialize(new TestArguments("amount"));
        } catch (DirectiveParseException e) {
            Assert.fail("Initialization failed: " + e.getMessage());
        }

        List<Row> result = directive.execute(rows, null);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testNullValuesIgnored() throws DirectiveExecutionException {
        List<Row> rows = new ArrayList<>();
        rows.add(new Row("amount", null));
        rows.add(new Row("amount", 100));
        rows.add(new Row("amount", null));
        rows.add(new Row("amount", 200));

        AggregateStats directive = new AggregateStats();
        try {
            directive.initialize(new TestArguments("amount"));
        } catch (DirectiveParseException e) {
            Assert.fail("Initialization failed: " + e.getMessage());
        }

        List<Row> result = directive.execute(rows, null);
        Row output = result.get(0);

        Assert.assertEquals(100.0, output.getValue("min"));
        Assert.assertEquals(200.0, output.getValue("max"));
        Assert.assertEquals(300.0, output.getValue("sum"));
        Assert.assertEquals(150.0, output.getValue("avg"));
        Assert.assertEquals(2L, output.getValue("count"));
    }

}
