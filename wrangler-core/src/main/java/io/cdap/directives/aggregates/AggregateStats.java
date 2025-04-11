package io.cdap.wrangler.directives.aggregates;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.DirectiveContext;
import io.cdap.wrangler.api.parser.DirectiveParseException;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.Value;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.DirectiveArgument;
import io.cdap.wrangler.api.parser.DirectiveDefinition;

import io.cdap.wrangler.api.parser.Arguments;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.OutputType;

import io.cdap.wrangler.api.parser.AssertionException;
import io.cdap.wrangler.api.parser.Assertion;
import io.cdap.wrangler.api.parser.Column;

import io.cdap.wrangler.api.parser.AssertionException;
import io.cdap.wrangler.api.parser.Assertion;

import io.cdap.wrangler.api.parser.ValueException;
import io.cdap.wrangler.api.parser.TextException;

import io.cdap.wrangler.api.parser.ColumnException;

import io.cdap.wrangler.api.parser.StringValue;

import io.cdap.wrangler.api.parser.IntegerValue;

import io.cdap.wrangler.api.parser.StringList;

import io.cdap.wrangler.api.parser.BooleanValue;

import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class AggregateStats implements Directive {
    private String sourceByteCol;
    private String sourceTimeCol;
    private String outputByteCol;
    private String outputTimeCol;

    private long totalBytes = 0;
    private long totalMilliseconds = 0;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .usage("aggregate-stats <byte-col> <time-col> <output-byte-col> <output-time-col>")
            .arguments(
                Arguments.of("byte-col", ColumnName.class),
                Arguments.of("time-col", ColumnName.class),
                Arguments.of("output-byte-col", Text.class),
                Arguments.of("output-time-col", Text.class)
            )
            .output(OutputType.AGGREGATE)
            .build();
    }

    @Override
    public void initialize(DirectiveContext ctx, Arguments args) throws DirectiveParseException {
        sourceByteCol = ((ColumnName) args.value("byte-col")).value();
        sourceTimeCol = ((ColumnName) args.value("time-col")).value();
        outputByteCol = ((Text) args.value("output-byte-col")).value();
        outputTimeCol = ((Text) args.value("output-time-col")).value();
    }

    @Override
    public void execute(Row row, ExecutorContext context) {
        Object byteObj = row.getValue(sourceByteCol);
        Object timeObj = row.getValue(sourceTimeCol);

        if (byteObj instanceof String) {
            ByteSize byteSize = new ByteSize((String) byteObj);
            totalBytes += byteSize.getBytes();
        }

        if (timeObj instanceof String) {
            TimeDuration timeDuration = new TimeDuration((String) timeObj);
            totalMilliseconds += timeDuration.getMilliseconds();
        }
    }

    @Override
    public List<Row> finalize(ExecutorContext context) {
        List<Row> results = new ArrayList<>();
        Row row = new Row();

        double totalMB = totalBytes / (1024.0 * 1024.0);
        double totalSeconds = totalMilliseconds / 1000.0;

        row.add(outputByteCol, totalMB);
        row.add(outputTimeCol, totalSeconds);

        results.add(row);
        return results;
    }
}