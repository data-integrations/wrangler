package io.cdap.wrangler.parser.directives.row;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.Directive;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.TokenGroup;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.ExecutorContext;

import java.util.List;
import java.util.ArrayList;

public class AggregateStats implements Directive {
    private String byteSizeCol;
    private String timeDurationCol;
    private String outputSizeCol;
    private String outputTimeCol;

    private long totalBytes = 0;
    private long totalMillis = 0;

    private boolean isFinalRun = false;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .withArgs(
                TokenGroup.builder()
                    .addTokenType(TokenType.COLUMN_NAME) // Byte size column
                    .addTokenType(TokenType.COLUMN_NAME) // Time duration column
                    .addTokenType(TokenType.TEXT)        // Output byte column
                    .addTokenType(TokenType.TEXT)        // Output time column
                    .build()
            )
            .build();
    }

    @Override
    public void initialize(ExecutorContext context, List<Object> args) throws Exception {
        this.byteSizeCol = ((ColumnName) args.get(0)).value();
        this.timeDurationCol = ((ColumnName) args.get(1)).value();
        this.outputSizeCol = ((Text) args.get(2)).value();
        this.outputTimeCol = ((Text) args.get(3)).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) throws Exception {
        List<Row> result = new ArrayList<>();

        for (Row row : rows) {
            Object sizeVal = row.getValue(byteSizeCol);
            Object timeVal = row.getValue(timeDurationCol);

            if (sizeVal != null && timeVal != null) {
                ByteSize size = new ByteSize(sizeVal.toString());
                TimeDuration time = new TimeDuration(timeVal.toString());

                totalBytes += size.getBytes();
                totalMillis += time.getMilliseconds();
            }
        }

        // Final aggregated row
        Row output = new Row();
        double mb = totalBytes / (1024.0 * 1024.0);
        double sec = totalMillis / 1000.0;

        output.add(outputSizeCol, mb);
        output.add(outputTimeCol, sec);

        result.add(output);
        return result;
    }
}
