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

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.*;
import java.util.List;

/**
 * Directive that aggregates byte size and time duration columns.
 */
public class AggregateStats implements Directive {

    private String sizeInputCol;
    private String timeInputCol;
    private String sizeOutputCol;
    private String timeOutputCol;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
                .define("sizeInputCol", TokenType.BYTE_SIZE)
                .define("timeInputCol", TokenType.TIME_DURATION)
                .define("sizeOutputCol", TokenType.BYTE_SIZE)
                .define("timeOutputCol", TokenType.TIME_DURATION);
                .build();
    }

    @Override
    public void initialize(Arguments arguments) throws DirectiveParseException {
        this.sizeInputCol = ((ColumnName) arguments.value("sizeInputCol")).value();
        this.timeInputCol = ((ColumnName) arguments.value("timeInputCol")).value();
        this.sizeOutputCol = ((ColumnName) arguments.value("sizeOutputCol")).value();
        this.timeOutputCol = ((ColumnName) arguments.value("timeOutputCol")).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        long totalBytes = 0;
        long totalMillis = 0;

        for (Row row : rows) {
            Object sizeVal = row.getValue(sizeInputCol);
            Object timeVal = row.getValue(timeInputCol);

            if (sizeVal instanceof String && timeVal instanceof String) {
                try {
                    long bytes = new ByteSize((String) sizeVal).getBytes();
                    long nanos = new TimeDuration((String) timeVal).getNanoseconds();
                    long millis = nanos / 1_000_000;

                    totalBytes += bytes;
                    totalMillis += millis;
                } catch (Exception e) {
                    throw new DirectiveExecutionException(e);
                }
            }
        }

        double mb = totalBytes / (1024.0 * 1024.0); // Convert to MB
        double seconds = totalMillis / 1000.0; // Convert to seconds

        Row output = new Row();
        output.add(sizeOutputCol, mb);
        output.add(timeOutputCol, seconds);

        return List.of(output);
    }

    @Override
    public void destroy() {
        // Nothing to clean up
    }
}
