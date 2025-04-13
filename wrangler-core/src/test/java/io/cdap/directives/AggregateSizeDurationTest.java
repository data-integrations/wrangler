/*
 *  Copyright © 2019 Cask Data, Inc.
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

package io.cdap.directives;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;

import static org.junit.Assert.assertEquals;

public class AggregateSizeDurationTest {

    @Test
    public void testTotalAggregation() throws Exception {
        AggregateSizeDuration directive = new AggregateSizeDuration();

        Arguments args = new TestArguments(Map.of(
                "sourceSizeCol", "size",
                "sourceTimeCol", "time",
                "targetSizeCol", "totalSize",
                "targetTimeCol", "totalTime",
                "sizeUnit", "KB",
                "timeUnit", "s",
                "aggType", "total"));
        directive.initialize(args);

        List<Row> input = Arrays.asList(
                new Row("size", "1024B").add("time", "1000ms"),
                new Row("size", "2048B").add("time", "2000ms"));

        List<Row> result = directive.execute(input, null);

        assertEquals(1, result.size());
        Row output = result.get(0);

        assertEquals(3.0, (double) output.getValue("totalSize"), 0.001);
        assertEquals(3.0, (double) output.getValue("totalTime"), 0.001);
    }

    @Test
    public void testAverageAggregation() throws Exception {
        AggregateSizeDuration directive = new AggregateSizeDuration();

        Arguments args = new TestArguments(Map.of(
                "sourceSizeCol", "size",
                "sourceTimeCol", "time",
                "targetSizeCol", "avgSize",
                "targetTimeCol", "avgTime",
                "sizeUnit", "KB",
                "timeUnit", "s",
                "aggType", "average"));
        directive.initialize(args);

        List<Row> input = Arrays.asList(
                new Row("size", "1024B").add("time", "1000ms"),
                new Row("size", "2048B").add("time", "2000ms"));

        List<Row> result = directive.execute(input, null);

        assertEquals(1, result.size());
        Row output = result.get(0);

        assertEquals(1.5, (double) output.getValue("avgSize"), 0.001);
        assertEquals(1.5, (double) output.getValue("avgTime"), 0.001);
    }

    // Minimal implementation of Arguments for testingstatic class TestArguments
    // implements Arguments {
    static class TestArguments implements Arguments {
        private final Map<String, String> values;

        TestArguments(Map<String, String> values) {
            this.values = new HashMap<>(values);
        }

        @Override
        public <T extends Token> T value(String name) {
            return null; // Mocked value for testing
        }

        @Override
        public boolean contains(String name) {
            return values.containsKey(name);
        }

        @Override
        public TokenType type(String name) {
            return TokenType.TEXT; // Placeholder, adjust as per your actual requirements
        }

        @Override
        public int line() {
            return 1; // Dummy value for testing
        }

        @Override
        public int column() {
            return 0; // Dummy value for testing
        }

        @Override
        public String source() {
            return "source-string"; // Placeholder for actual logic
        }

        public String column(String name) {
            return values.getOrDefault(name, null);
        }

        public String asString() {
            return values.toString(); // Return string representation of the map
        }

        public Map<String, String> asMap() {
            return new HashMap<>(values); // Return a copy of the map
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public JsonElement toJson() {
            JsonObject json = new JsonObject();
            values.forEach(json::addProperty);
            return json; // Convert the map to a JSON object
        }
    }

}
