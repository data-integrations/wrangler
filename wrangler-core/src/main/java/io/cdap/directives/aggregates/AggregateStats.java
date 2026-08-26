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
 * Directive for aggregating statistics.
 */
@Categories(categories = { "data-aggregation"})

public class AggregateStats implements Directive {
    public static final String NAME = "aggregate-stats";
    private String byteCol;
    private String timeCol;
    private String outputSizeCol;
    private String outputTimeCol;
    private double totalBytes = 0.0;
    private double totalTimeMs = 0.0;
    private int count = 0;

    /**
     * Defines the usage of the directive, specifying the required arguments.
     *
     * @return UsageDefinition object containing the directive's argument definitions.
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
     * Initializes the directive with the provided arguments.
     *
     * @param args Arguments object containing the directive's parameters.
     * @throws DirectiveParseException if there is an error parsing the arguments.
     */
    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        byteCol = ((ColumnName) args.value("byteCol")).value();
        timeCol = ((ColumnName) args.value("timeCol")).value();
        outputSizeCol = ((Text) args.value("outputSizeCol")).value();
        outputTimeCol = ((Text) args.value("outputTimeCol")).value();
    }

    /**
     * Executes the directive on a list of rows, aggregating statistics based on the specified columns.
     *
     * @param rows List of Row objects to process.
     * @param ctx ExecutorContext providing execution context information.
     * @return List of Row objects containing the aggregated results.
     * @throws DirectiveExecutionException if there is an error during execution.
     */
    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext ctx) throws DirectiveExecutionException {
        try {
            for (Row row : rows) {
                if (row.find(byteCol) != -1 && row.find(timeCol) != -1) {
                    String byteVal = row.getValue(byteCol).toString();
                    String timeVal = row.getValue(timeCol).toString();
                    
                    // Use ByteSize and TimeDuration classes for parsing
                    ByteSize byteSize = new ByteSize(byteVal);
                    TimeDuration duration = new TimeDuration(timeVal);
                    
                    totalBytes += byteSize.getBytes();
                    totalTimeMs += duration.getTime();
                    count++;
                }
            }

            if (count == 0) {
                return new ArrayList<>(); 
            }

            List<Row> results = new ArrayList<>();
            Row result = new Row();
            
            // Convert bytes to MB and milliseconds to seconds
            result.add(outputSizeCol, totalBytes / (1024.0 * 1024.0));
            result.add(outputTimeCol, totalTimeMs / 1000.0);
            
            results.add(result);
            return results;
            
        } catch (Exception e) {
            throw new DirectiveExecutionException(
                String.format("Error aggregating stats: %s", e.getMessage())
            );
        }
    }

    /**
     * Provides the output schema for the directive based on the input schema.
     *
     * @param inputSchema Schema object representing the input schema.
     * @return Schema object representing the output schema.
     */
    public Schema getOutputSchema(Schema inputSchema) {
        List<Schema.Field> fields = new ArrayList<>();
        fields.add(Schema.Field.of(outputSizeCol, Schema.of(Schema.Type.DOUBLE)));
        fields.add(Schema.Field.of(outputTimeCol, Schema.of(Schema.Type.DOUBLE)));
        return Schema.recordOf("aggregate-stats", fields);
    }

    /**
     * Cleans up resources and resets the directive's state.
     */
    @Override
    public void destroy() {
      // Reset all accumulated values
      totalBytes = 0.0;
      totalTimeMs = 0.0;
      count = 0;

      // Clear column references
      byteCol = null;
      timeCol = null;
      outputSizeCol = null;
      outputTimeCol = null;
    }
}
