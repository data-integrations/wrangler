/*
 * Copyright © 2019 Cask Data, Inc.
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
/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.wrangler.directives.aggregates;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.Arguments;
import io.cdap.wrangler.api.parser.OutputType;
import java.util.ArrayList;
import java.util.List;

/**
 * Directive to aggregate byte sizes and time durations.
 * Sums up values from a source byte-size column and a source time column, and outputs aggregated totals.
 */
public class AggregateStats implements Directive {

    private String sourceByteCol;
    private String sourceTimeCol;
    private String outputByteCol;
    private String outputTimeCol;

    // Accumulators for totals.
    private long totalBytes = 0;
    private long totalMilliseconds = 0;

    @Override
    public UsageDefinition define() {
        // Use description() instead of usage() if your API supports it.
        return UsageDefinition.builder("aggregate-stats")
                .description("aggregate-stats <byte-col> <time-col> <output-byte-col> <output-time-col>")
                .arguments(
                        Arguments.of("byte-col", ColumnName.class),       // e.g., ":data_transfer_size"
                        Arguments.of("time-col", ColumnName.class),         // e.g., ":response_time"
                        Arguments.of("output-byte-col", Text.class),        // e.g., "total_size_mb"
                        Arguments.of("output-time-col", Text.class)         // e.g., "total_time_sec"
                )
                .output(OutputType.AGGREGATE)
                .build();
    }

    @Override
    public void initialize(DirectiveContext ctx, Arguments args) throws Exception {
        // Extract the provided argument values.
        sourceByteCol = ((ColumnName) args.value("byte-col")).value();
        sourceTimeCol = ((ColumnName) args.value("time-col")).value();
        outputByteCol = ((Text) args.value("output-byte-col")).value();
        outputTimeCol = ((Text) args.value("output-time-col")).value();
    }

    /**
     * This method processes a list of rows. It replaces the older single‑row execute().
     */
    @Override
    public void execute(List<Row> rows, ExecutorContext context) throws Exception {
        // Process each row in the list.
        for (Row row : rows) {
            Object byteObj = row.getValue(sourceByteCol);
            Object timeObj = row.getValue(sourceTimeCol);

            // Process byte value: if it's a string, parse using ByteSize; if numeric, add directly.
            if (byteObj instanceof String) {
                ByteSize byteSize = new ByteSize((String) byteObj);
                totalBytes += byteSize.getBytes();
            } else if (byteObj instanceof Number) {
                totalBytes += ((Number) byteObj).longValue();
            }

            // Process time value: if string, parse using TimeDuration; if numeric, assume milliseconds.
            if (timeObj instanceof String) {
                TimeDuration timeDuration = new TimeDuration((String) timeObj);
                // Use the API’s method to convert to milliseconds.
                totalMilliseconds += timeDuration.toMilliseconds();
            } else if (timeObj instanceof Number) {
                totalMilliseconds += ((Number) timeObj).longValue();
            }
        }
    }

    @Override
    public List<Row> finalize(ExecutorContext context) throws Exception {
        // Create a new row that stores the aggregation results.
        List<Row> results = new ArrayList<>();
        Row resultRow = new Row();

        // Convert totals to output units: bytes -> MB and milliseconds -> seconds.
        double totalMB = totalBytes / (1024.0 * 1024.0);
        double totalSeconds = totalMilliseconds / 1000.0;
        resultRow.add(outputByteCol, totalMB);
        resultRow.add(outputTimeCol, totalSeconds);

        results.add(resultRow);
        return results;
    }

    @Override
    public void destroy() {
        // No cleanup necessary.
    }
}
