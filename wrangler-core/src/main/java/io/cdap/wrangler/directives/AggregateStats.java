package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.row.Row;
import io.cdap.wrangler.api.executor.Executor;
import io.cdap.wrangler.api.Arguments;

import java.util.ArrayList;
import java.util.List;

@DirectiveInfo(
  name = "aggregate-stats",
  description = "Aggregates byte size and time duration columns and stores the totals in new columns",
  type = DirectiveType.AGGREGATE
)
public class AggregateStats implements Directive, Executor<List<Row>, List<Row>> {

  private String sizeColumn;
  private String timeColumn;
  private String outputSizeColumn;
  private String outputTimeColumn;

  private long totalBytes = 0;
  private long totalNanos = 0;
  private int rowCount = 0;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder()
      .define("sizeColumn", TokenType.COLUMN_NAME)
      .define("timeColumn", TokenType.COLUMN_NAME)
      .define("outputSizeColumn", TokenType.COLUMN_NAME)
      .define("outputTimeColumn", TokenType.COLUMN_NAME)
      .build();
  }

  @Override
  public void initialize(Arguments arguments) {
    sizeColumn = arguments.value("sizeColumn");
    timeColumn = arguments.value("timeColumn");
    outputSizeColumn = arguments.value("outputSizeColumn");
    outputTimeColumn = arguments.value("outputTimeColumn");
  }

  @Override
  public List<Row> execute(List<Row> rows) throws DirectiveExecuteException {
    for (Row row : rows) {
      Object sizeVal = row.getValue(sizeColumn);
      Object timeVal = row.getValue(timeColumn);

      if (sizeVal instanceof String) {
        ByteSize byteSize = new ByteSize((String) sizeVal);
        totalBytes += byteSize.getBytes();
      }

      if (timeVal instanceof String) {
        TimeDuration timeDuration = new TimeDuration((String) timeVal);
        totalNanos += timeDuration.getNanoseconds();
      }

      rowCount++;
    }

    double totalSizeMB = totalBytes / (1024.0 * 1024.0); // MB
    double totalTimeSec = totalNanos / 1_000_000_000.0;  // Seconds

    Row result = new Row();
    result.add(outputSizeColumn, totalSizeMB);
    result.add(outputTimeColumn, totalTimeSec);

    List<Row> output = new ArrayList<>();
    output.add(result);
    return output;
  }

  @Override
  public void destroy() {
    // Clean up if needed
  }
}
