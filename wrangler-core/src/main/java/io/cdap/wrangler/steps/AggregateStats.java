package io.cdap.wrangler.steps;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.Step;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Token;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.util.List;
import java.util.ArrayList;

public class AggregateStats implements Directive {
    private String sizeColumn;
    private String timeColumn;
    private String outputSizeCol;
    private String outputTimeCol;

    private long totalBytes = 0;
    private long totalMilliseconds = 0;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder()
            .define("aggregate-stats")
            .withRequiredArg("sizeColumn")
            .withRequiredArg("timeColumn")
            .withRequiredArg("outputSizeCol")
            .withRequiredArg("outputTimeCol")
            .build();
    }

    @Override
    public void initialize(Arguments args) {
        this.sizeColumn = args.value("sizeColumn");
        this.timeColumn = args.value("timeColumn");
        this.outputSizeCol = args.value("outputSizeCol");
        this.outputTimeCol = args.value("outputTimeCol");
    }

    @Override
    public List<Row> execute(List<Row> rows) {
        for (Row row : rows) {
            String sizeStr = row.getValue(sizeColumn).toString();
            String timeStr = row.getValue(timeColumn).toString();
            totalBytes += new ByteSize(sizeStr).getBytes();
            totalMilliseconds += new TimeDuration(timeStr).getMilliseconds();
        }

        Row output = new Row();
        output.add(outputSizeCol, totalBytes / (1024.0 * 1024)); // Convert to MB
        output.add(outputTimeCol, totalMilliseconds / 1000.0); // Convert to seconds

        List<Row> result = new ArrayList<>();
        result.add(output);
        return result;
    }
}
