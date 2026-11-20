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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.Collections;
import java.util.List;

@PublicEvolving
public class AggregateStats implements Directive {
    private String sizeCol;
    private String timeCol;
    private String outputSizeCol;
    private String outputTimeCol;
    private long totalSize = 0;
    private long totalTime = 0;

    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");

        builder.define("sizeColumn", TokenType.COLUMN_NAME);
        builder.define("timeColumn", TokenType.COLUMN_NAME);
        builder.define("outputSizeColumn", TokenType.TEXT);
        builder.define("outputTimeColumn", TokenType.TEXT);

        return builder.build();
    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        sizeCol = ((ColumnName) args.value("sizeColumn")).value();
        timeCol = ((ColumnName) args.value("timeColumn")).value();
        outputSizeCol = ((Text) args.value("outputSizeColumn")).value();
        outputTimeCol = ((Text) args.value("outputTimeColumn")).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
        for (Row row : rows) {
            Object sizeVal = row.getValue(sizeCol);
            Object timeVal = row.getValue(timeCol);

            if (sizeVal instanceof Long) {
                totalSize += (Long) sizeVal;
            }

            if (timeVal instanceof Long) {
                totalTime += (Long) timeVal;
            }
        }

        Row result = new Row();
        result.add(outputSizeCol, totalSize / (1024 * 1024)); // Convert bytes to MB
        result.add(outputTimeCol, totalTime / 1000);          // Convert ms to seconds

        return Collections.singletonList(result);
    }

    @Override
    public void destroy() {
        // no-op
    }
}