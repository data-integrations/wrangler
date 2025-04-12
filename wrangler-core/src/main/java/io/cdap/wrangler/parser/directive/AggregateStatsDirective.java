package io.cdap.wrangler.parser.directive;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

public class AggregateStatsDirective implements Directive {
  private String sourceSizeColumn;
  private String sourceTimeColumn;
  private String targetSizeColumn;
  private String targetTimeColumn;
  private String aggregationOperation;

  @Override
  public void initialize(Arguments arguments) {
    // Parse the arguments
    this.sourceSizeColumn = arguments.value("source_size").toString();
    this.sourceTimeColumn = arguments.value("source_time").toString();
    this.targetSizeColumn = arguments.value("target_size").toString();
    this.targetTimeColumn = arguments.value("target_time").toString();
    this.aggregationOperation = arguments.value("operation") != null ? arguments.value("operation").toString() : "sum"; // Default to "sum"
  }

  @Override
  public void destroy() {
    // No resources to clean up
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) {
    long totalSize = 0;
    long totalTime = 0;
    List<Long> sizeValues = new ArrayList<>();
    List<Long> timeValues = new ArrayList<>();

    // Iterate through the rows and collect size and time values
    for (Row row : rows) {
      Object sizeObj = row.getValue(sourceSizeColumn);
      Object timeObj = row.getValue(sourceTimeColumn);

      // Aggregation for size values (e.g., ByteSize)
      if (sizeObj instanceof ByteSize) {
        sizeValues.add(((ByteSize) sizeObj).getBytes());
      }

      // Aggregation for time values (e.g., TimeDuration)
      if (timeObj instanceof TimeDuration) {
        timeValues.add(((TimeDuration) timeObj).getNanos());
      }
    }

    // Perform the aggregation operation
    switch (aggregationOperation.toLowerCase()) {
      case "sum":
        totalSize = sizeValues.stream().mapToLong(Long::longValue).sum();
        totalTime = timeValues.stream().mapToLong(Long::longValue).sum();
        break;
      case "average":
        totalSize = (long) sizeValues.stream().mapToLong(Long::longValue).average().orElse(0);
        totalTime = (long) timeValues.stream().mapToLong(Long::longValue).average().orElse(0);
        break;
      case "median":
        totalSize = calculateMedian(sizeValues);
        totalTime = calculateMedian(timeValues);
        break;
      case "p95":
        totalSize = calculatePercentile(sizeValues, 95);
        totalTime = calculatePercentile(timeValues, 95);
        break;
      case "p99":
        totalSize = calculatePercentile(sizeValues, 99);
        totalTime = calculatePercentile(timeValues, 99);
        break;
      default:
        throw new IllegalArgumentException("Invalid aggregation operation: " + aggregationOperation);
    }

    // Convert size to MB and time to seconds
    double totalSizeMB = convertBytesToMB(totalSize);
    double totalTimeSec = convertNanosToSeconds(totalTime);

    // Create a new row with the aggregated values
    Row newRow = new Row();
    newRow.add(targetSizeColumn, totalSizeMB);
    newRow.add(targetTimeColumn, totalTimeSec);
    return List.of(newRow);
  }

  private long calculateMedian(List<Long> values) {
    if (values.isEmpty()) {
      return 0;
    }
    Collections.sort(values);
    int size = values.size();
    if (size % 2 == 0) {
      return (values.get(size / 2 - 1) + values.get(size / 2)) / 2;
    } else {
      return values.get(size / 2);
    }
  }

  private long calculatePercentile(List<Long> values, double percentile) {
    if (values.isEmpty()) {
      return 0;
    }
    Collections.sort(values);
    int index = (int) Math.ceil(percentile / 100 * values.size()) - 1;
    return values.get(index);
  }

  private double convertBytesToMB(long bytes) {
    // Assuming 1 MB = 1024 * 1024 bytes (1024-based conversion)
    return bytes / (1024.0 * 1024);
  }

  private double convertNanosToSeconds(long nanos) {
    return nanos / 1_000_000_000.0;  // Convert nanoseconds to seconds
  }

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
    builder.define("source_size", TokenType.COLUMN_NAME);
    builder.define("source_time", TokenType.COLUMN_NAME);
    builder.define("target_size", TokenType.COLUMN_NAME);
    builder.define("target_time", TokenType.COLUMN_NAME);
    builder.define("operation", TokenType.TEXT);  // Define the operation (sum, average, etc.)
    return builder.build();
  }
}
