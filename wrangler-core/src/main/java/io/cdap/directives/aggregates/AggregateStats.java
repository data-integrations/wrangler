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

import io.cdap.cdap.api.annotation.Name;

import io.cdap.cdap.api.annotation.Plugin;

import io.cdap.cdap.api.data.schema.Schema;

import io.cdap.wrangler.api.Arguments;

import io.cdap.wrangler.api.Directive;

import io.cdap.wrangler.api.DirectiveExecutionException;

import io.cdap.wrangler.api.DirectiveParseException;

import io.cdap.wrangler.api.ExecutorContext;

import io.cdap.wrangler.api.Row;

import io.cdap.wrangler.api.annotations.Categories;

import io.cdap.wrangler.api.parser.ByteSize;

import io.cdap.wrangler.api.parser.ColumnName;

import io.cdap.wrangler.api.parser.Text;

import io.cdap.wrangler.api.parser.TimeDuration;

import io.cdap.wrangler.api.parser.TokenType;

import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.ArrayList;

import java.util.List;
/**
 * Directive to aggregate statistics over a dataset. It computes
 * total bytes and total time, then outputs the summary in MB and seconds.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Categories(categories = { "data-aggregation" })
public class AggregateStats implements Directive {

    public static final String NAME = "aggregate-stats";

    // Input column names
    private String inputByteColumn;
    private String inputTimeColumn;

    // Output column names
    private String resultByteColumn;
    private String resultTimeColumn;

    // Aggregated values
    private double accumulatedBytes = 0.0;
    private double accumulatedTimeMs = 0.0;
    private int processedRowCount = 0;

    /**
     * Defines the parameters required to use this directive.
     *
     * @return UsageDefinition instance outlining directive inputs.
     */
    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("byteCol", TokenType.COLUMN_NAME);
        builder.define("timeCol", TokenType.COLUMN_NAME);
        builder.define("outputSizeCol", TokenType.TEXT);
        builder.define("outputTimeCol", TokenType.TEXT);
        return builder.build();
    }

    /**
     * Initializes the directive with user-provided arguments.
     *
     * @param args The input arguments.
     * @throws DirectiveParseException if parsing fails.
     */
    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        inputByteColumn = ((ColumnName) args.value("byteCol")).value();
        inputTimeColumn = ((ColumnName) args.value("timeCol")).value();
        resultByteColumn = ((Text) args.value("outputSizeCol")).value();
        resultTimeColumn = ((Text) args.value("outputTimeCol")).value();
    }

    /**
     * Executes aggregation logic over input rows.
     *
     * @param rows List of input rows.
     * @param ctx  Execution context.
     * @return List containing a single row with aggregated metrics.
     * @throws DirectiveExecutionException if any error occurs during execution.
     */
    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext ctx) throws DirectiveExecutionException {
        try {
            for (Row row : rows) {
                if (row.find(inputByteColumn) != -1 && row.find(inputTimeColumn) != -1) {
                    String byteValue = row.getValue(inputByteColumn).toString();
                    String timeValue = row.getValue(inputTimeColumn).toString();

                    // Parse input values
                    ByteSize byteSize = new ByteSize(byteValue);
                    TimeDuration duration = new TimeDuration(timeValue);

                    accumulatedBytes += byteSize.getBytes();
                    accumulatedTimeMs += duration.getValue();
                    processedRowCount++;
                }
            }

            if (processedRowCount == 0) {
                return new ArrayList<>();
            }

            // Prepare output
            List<Row> results = new ArrayList<>();
            Row resultRow = new Row();

            resultRow.add(resultByteColumn, accumulatedBytes / (1024.0 * 1024.0));  // Convert bytes to MB
            resultRow.add(resultTimeColumn, accumulatedTimeMs / 1000.0);            // Convert ms to seconds

            results.add(resultRow);
            return results;

        } catch (Exception e) {
            throw new DirectiveExecutionException(
                String.format("Error aggregating stats: %s", e.getMessage())
            );
        }
    }

    /**
     * Defines the output schema for the transformed dataset.
     *
     * @param inputSchema The schema of incoming data.
     * @return Schema of the resulting data.
     */
    public Schema getOutputSchema(Schema inputSchema) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of(resultByteColumn, Schema.of(Schema.Type.DOUBLE)));
        fields.add(Schema.Field.of(resultTimeColumn, Schema.of(Schema.Type.DOUBLE)));
        return Schema.recordOf("aggregate-stats", fields);
    }

    /**
     * Cleans up and resets the internal state of the directive.
     */
    @Override
    public void destroy() {
        accumulatedBytes = 0.0;
        accumulatedTimeMs = 0.0;
        processedRowCount = 0;

        inputByteColumn = null;
        inputTimeColumn = null;
        resultByteColumn = null;
        resultTimeColumn = null;
    }
}
