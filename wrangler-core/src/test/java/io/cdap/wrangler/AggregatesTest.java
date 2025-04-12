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
package io.cdap.wrangler;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.directives.aggregates.Aggregates;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatesTest {

    @Test
    public void testAggregationTotalMode() throws Exception {
        Aggregates directive = new Aggregates();

        Arguments arguments = new Arguments() {
            private final Map<String, Token> args = new HashMap<>();
            {
                args.put("sizeCol", new ColumnName("dataSize"));
                args.put("timeCol", new ColumnName("elapsedTime"));
                args.put("outputSizeCol", new ColumnName("totalSize"));
                args.put("outputTimeCol", new ColumnName("totalTime"));
                args.put("aggregationType", new ColumnName("total")); // Using ColumnName for simplicity
                args.put("outputSizeUnit", new ColumnName("MB"));
                args.put("outputTimeUnit", new ColumnName("seconds"));
            }

            @Override
            public <T extends Token> T value(String name) {
                return (T) args.get(name);
            }

            @Override
            public int size() {
                return args.size();
            }

            @Override
            public boolean contains(String name) {
                return args.containsKey(name);
            }

            @Override
            public TokenType type(String name) {
                return args.containsKey(name) ? args.get(name).type() : null;
            }

            @Override
            public int line() {
                return 1; // Stubbed for test
            }

            @Override
            public int column() {
                return 1; // Stubbed for test
            }

            @Override
            public String source() {
                return "aggregate-size-duration dataSize elapsedTime totalSize totalTime total MB seconds";
            }

            @Override
            public JsonElement toJson() {
                JsonObject json = new JsonObject();
                for (Map.Entry<String, Token> entry : args.entrySet()) {
                    json.add(entry.getKey(), entry.getValue().toJson());
                }
                return json;
            }
        };

        directive.initialize(arguments);

        List<Row> input = new ArrayList<>();
        Row row1 = new Row();
        row1.add("dataSize", "1MB");
        row1.add("elapsedTime", "1s");

        Row row2 = new Row();
        row2.add("dataSize", "2MB");
        row2.add("elapsedTime", "2s");

        input.add(row1);
        input.add(row2);


        ExecutorContext context = new TestingPipelineContext();
        List<Row> result = directive.execute(input, context);

        Row aggregated = result.get(0);
        Assert.assertEquals("3.0 MB", aggregated.getValue("totalSize"));
        Assert.assertEquals("3.0 s", aggregated.getValue("totalTime"));
    }
}
