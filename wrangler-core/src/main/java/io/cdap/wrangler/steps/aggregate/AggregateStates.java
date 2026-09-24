package io.cdap.wrangler.steps.aggregate;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.*;
import java.util.List;

public class AggregateStats implements Directive, AggregateDirective {
  private String sourceSizeCol;
  private String sourceTimeCol;
  private String targetSizeCol;
  private String targetTimeCol;
  private String outputSizeUnit = "B";     // Optional, default is bytes
  private String outputTimeUnit = "ms";    // Optional, default is milliseconds
  private boolean average = false;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats")
      .define("source_size", TokenType.COLUMN_NAME)
      .define("source_time", TokenType.COLUMN_NAME)
      .define("target_size", TokenType.COLUMN_NAME)
      .define("target_time", TokenType.COLUMN_NAME)
      .defineOptional("output_size_unit", TokenType.TEXT)     // "MB", "GB", etc.
      .defineOptional("output_time_unit", TokenType.TEXT)     // "s", "min", etc.
      .defineOptional("average", TokenType.BOOLEAN)           // true or false
      .build();
  }

  @Override
  public void initialize(Arguments args) {
    sourceSizeCol = ((ColumnName) args.value("source_size")).value();
    sourceTimeCol = ((ColumnName) args.value("source_time")).value();
    targetSizeCol = ((ColumnName) args.value("target_size")).value();
    targetTimeCol = ((ColumnName) args.value("target_time")).value();

    if (args.contains("output_size_unit")) {
      outputSizeUnit = ((Text) args.value("output_size_unit")).value();
    }

    if (args.contains("output_time_unit")) {
      outputTimeUnit = ((Text) args.value("output_time_unit")).value();
    }

    if (args.contains("average")) {
      average = ((Bool) args.value("average")).value();
    }
  }

  @Override
  public AggregateResult execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    Store store = context.getStore("aggregate-stats");
    long totalBytes = store.getOrDefault("totalBytes", 0L);
    long totalMillis = store.getOrDefault("totalMillis", 0L);
    int count = store.getOrDefault("count", 0);

    for (Row row : rows) {
      Object byteObj = row.getValue(sourceSizeCol);
      Object timeObj = row.getValue(sourceTimeCol);

      if (byteObj instanceof Long) {
        totalBytes += (Long) byteObj;
      } else if (byteObj instanceof ByteSize) {
        totalBytes += ((ByteSize) byteObj).getBytes();
      }

      if (timeObj instanceof Long) {
        totalMillis += (Long) timeObj;
      } else if (timeObj instanceof TimeDuration) {
        totalMillis += ((TimeDuration) timeObj).getMilliseconds();
      }

      count++;
    }

    // Save back to store
    store.set("totalBytes", totalBytes);
    store.set("totalMillis", totalMillis);
    store.set("count", count);

    return AggregateResult.PASSTHROUGH;
  }

  @Override
  public AggregateResult finalize(ExecutorContext context) {
    Store store = context.getStore("aggregate-stats");
    long totalBytes = store.getOrDefault("totalBytes", 0L);
    long totalMillis = store.getOrDefault("totalMillis", 0L);
    int count = store.getOrDefault("count", 1);  // Avoid div-by-zero

    if (average && count > 0) {
      totalBytes /= count;
      totalMillis /= count;
    }

    double finalBytes = convertBytes(totalBytes, outputSizeUnit);
    double finalTime = convertTime(totalMillis, outputTimeUnit);

    Row output = new Row();
    output.add(targetSizeCol, finalBytes);
    output.add(targetTimeCol, finalTime);

    return AggregateResult.output(output);
  }

  private double convertBytes(long bytes, String unit) {
    switch (unit.toLowerCase()) {
      case "kb": return bytes / 1024.0;
      case "mb": return bytes / (1024.0 * 1024);
      case "gb": return bytes / (1024.0 * 1024 * 1024);
      case "tb": return bytes / (1024.0 * 1024 * 1024 * 1024);
      default: return bytes;
    }
  }

  private double convertTime(long millis, String unit) {
    switch (unit.toLowerCase()) {
      case "s": return millis / 1000.0;
      case "min": return millis / (60.0 * 1000);
      case "h": return millis / (60.0 * 60 * 1000);
      default: return millis;
    }
  }
}
