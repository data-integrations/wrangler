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
    if ("sum".equalsIgnoreCase(aggregationOperation)) {
      totalSize = sizeValues.stream().mapToLong(Long::longValue).sum();
      totalTime = timeValues.stream().mapToLong(Long::longValue).sum();
    } else if ("average".equalsIgnoreCase(aggregationOperation)) {
      totalSize = (long) sizeValues.stream().mapToLong(Long::longValue).average().orElse(0);
      totalTime = (long) timeValues.stream().mapToLong(Long::longValue).average().orElse(0);
    } else if ("median".equalsIgnoreCase(aggregationOperation)) {
      totalSize = calculateMedian(sizeValues);
      totalTime = calculateMedian(timeValues);
    }
    // Add more operations like p95, p99 if necessary

    // Create a new row with the aggregated values
    Row newRow = new Row();
    newRow.add(targetSizeColumn, totalSize);
    newRow.add(targetTimeColumn, totalTime);
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

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
    builder.define("source_size", TokenType.COLUMN_NAME);
    builder.define("source_time", TokenType.COLUMN_NAME);
    builder.define("target_size", TokenType.COLUMN_NAME);
    builder.define("target_time", TokenType.COLUMN_NAME);
    return builder.build();
  }
}
