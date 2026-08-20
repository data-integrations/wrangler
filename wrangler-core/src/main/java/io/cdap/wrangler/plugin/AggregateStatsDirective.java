/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.wrangler.plugin;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

// ✅ THIS WORKS in your version

import java.util.Collections;
import java.util.List;

public class AggregateStatsDirective implements Directive {

    private String sizeCol;
    private String timeCol;
    private String targetSizeCol;
    private String targetTimeCol;

    private long totalBytes = 0;
    private long totalMillis = 0;

    // ✅ .accepts(...) is the correct method for your older UsageDefinition
    @Override
    public io.cdap.wrangler.api.parser.UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats", "Aggregate byte and time data");

        builder.define("size_col", TokenType.COLUMN_NAME);
        builder.define("time_col", TokenType.COLUMN_NAME);
        builder.define("total_size_col", TokenType.COLUMN_NAME);
        builder.define("total_time_col", TokenType.COLUMN_NAME);

        return builder.build();
    }









    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        sizeCol = arguments.value("size_col");
        timeCol = arguments.value("time_col");
        targetSizeCol = arguments.value("total_size_col");
        targetTimeCol = arguments.value("total_time_col");
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        for (Row row : rows) {
            Object sizeObj = row.getValue(sizeCol);
            Object timeObj = row.getValue(timeCol);

            long bytes = parseByteSize(sizeObj.toString());
            long millis = parseTimeDuration(timeObj.toString());

            totalBytes += bytes;
            totalMillis += millis;
        }

        double totalMB = totalBytes / (1024.0 * 1024.0);
        double totalSeconds = totalMillis / 1000.0;


        Row result = new Row();
        result.add(targetSizeCol, totalMB);
        result.add(targetTimeCol, totalSeconds);

        return Collections.singletonList(result);
    }

    @Override
    public void destroy() {
        // Optional cleanup
    }

    private long parseByteSize(String input) {
        input = input.trim().toUpperCase();
        if (input.endsWith("KB")) {
            return (long) (Double.parseDouble(input.replace("KB", "")) * 1024);
        } else if (input.endsWith("MB")) {
            return (long) (Double.parseDouble(input.replace("MB", "")) * 1024 * 1024);
        } else if (input.endsWith("GB")) {
            return (long) (Double.parseDouble(input.replace("GB", "")) * 1024 * 1024 * 1024);
        } else if (input.endsWith("B")) {
            return Long.parseLong(input.replace("B", ""));
        }
        return 0;
    }

    private long parseTimeDuration(String input) {
        input = input.trim().toLowerCase();
        if (input.endsWith("ms")) {
            return (long) Double.parseDouble(input.replace("ms", ""));
        } else if (input.endsWith("s")) {
            return (long) (Double.parseDouble(input.replace("s", "")) * 1000);
        } else if (input.endsWith("m")) {
            return (long) (Double.parseDouble(input.replace("m", "")) * 60 * 1000);
        } else if (input.endsWith("h")) {
            return (long) (Double.parseDouble(input.replace("h", "")) * 3600 * 1000);
        }
        return 0;
    }
}
