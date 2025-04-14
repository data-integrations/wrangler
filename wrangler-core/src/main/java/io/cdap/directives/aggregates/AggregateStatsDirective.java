package io.cdap.directives;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.List;

public class AggregateStatsDirective implements Directive, Aggregate {
    private String sizeCol, timeCol, totalSizeCol, totalTimeCol;
    private long totalSize = 0;
    private long totalTime = 0;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder()
            .name("aggregate-stats")
            .define("sizeCol", TokenType.STRING)
            .define("timeCol", TokenType.STRING)
            .define("totalSizeCol", TokenType.STRING)
            .define("totalTimeCol", TokenType.STRING)
            .build();
    }

    @Override
    public void initialize(Arguments args) {
        sizeCol = args.value("sizeCol");
        timeCol = args.value("timeCol");
        totalSizeCol = args.value("totalSizeCol");
        totalTimeCol = args.value("totalTimeCol");
    }

    @Override
    public void accumulate(Row row) {
        String sizeValue = (String) row.getValue(sizeCol);
        String timeValue = (String) row.getValue(timeCol);

        totalSize += new ByteSize(sizeValue).getBytes();
        totalTime += new TimeDuration(timeValue).getMilliseconds();
    }

    @Override
    public List<Row> rows() {
        Row row = new Row();
        row.add(totalSizeCol, totalSize / (1024.0 * 1024)); // to MB
        row.add(totalTimeCol, totalTime / 1000.0); // to seconds
        return List.of(row);
    }
}