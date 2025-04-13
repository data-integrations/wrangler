package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.executor.ExecutorContext;

import java.util.*;
import java.util.stream.Collectors;

public class AggregateStats implements Directive, Initializer {

  private String sizeCol;
  private String timeCol;
  private String targetSizeCol;
  private String targetTimeCol;
  private String sizeUnit = "bytes";
  private String timeUnit = "ns";
  private String aggType = "total";

  private long totalSize = 0;
  private long totalTime = 0;
  private int count = 0;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder()
      .define("size_col", TokenType.COLUMN_NAME)
      .define("time_col", TokenType.COLUMN_NAME)
      .define("target_size_col", TokenType.COLUMN_NAME)
      .define("target_time_col", TokenType.COLUMN_NAME)
      .defineOptional("size_unit", TokenType.TEXT)
      .defineOptional("time_unit", TokenType.TEXT)
      .defineOptional("agg_type", TokenType.TEXT); // total or average

    return builder.build();
  }

  @Override
  public void initialize(Arguments arguments) {
    this.sizeCol = ((ColumnName) arguments.value("size_col")).value();
    this.timeCol = ((ColumnName) arguments.value("time_col")).value();
    this.targetSizeCol = ((ColumnName) arguments.value("target_size_col")).value();
    this.targetTimeCol = ((ColumnName) arguments.value("target_time_col")).value();

    if (arguments.contains("size_unit")) {
      this.sizeUnit = ((Text) arguments.value("size_unit")).value().toLowerCase();
    }

    if (arguments.contains("time_unit")) {
      this.timeUnit = ((Text) arguments.value("time_unit")).value().toLowerCase();
    }

    if (arguments.contains("agg_type")) {
      this.aggType = ((Text) arguments.value("agg_type")).value().toLowerCase();
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) {
    for (Row row : rows) {
      Object sizeVal = row.getValue(sizeCol);
      Object timeVal = row.getValue(timeCol);

      if (sizeVal instanceof String && timeVal instanceof String) {
        ByteSize byteSize = new ByteSize((String) sizeVal);
        TimeDuration timeDuration = new TimeDuration((String) timeVal);

        totalSize += byteSize.getBytes();
        totalTime += timeDuration.getNanoseconds();
        count++;
      }
    }

    long resultSize = aggType.equals("average") ? totalSize / count : totalSize;
    long resultTime = aggType.equals("average") ? totalTime / count : totalTime;

    double convertedSize = convertBytes(resultSize, sizeUnit);
    double convertedTime = convertNanos(resultTime, timeUnit);

    Row resultRow = new Row();
    resultRow.add(targetSizeCol, convertedSize);
    resultRow.add(targetTimeCol, convertedTime);

    return Collections.singletonList(resultRow);
  }

  private double convertBytes(long bytes, String unit) {
    switch (unit) {
      case "kb": return bytes / 1024.0;
      case "mb": return bytes / (1024.0 * 1024);
      case "gb": return bytes / (1024.0 * 1024 * 1024);
      default: return bytes;
    }
  }

  private double convertNanos(long nanos, String unit) {
    switch (unit) {
      case "ms": return nanos / 1_000_000.0;
      case "s": return nanos / 1_000_000_000.0;
      case "minutes": return nanos / (60_000_000_000.0);
      default: return nanos;
    }
  }
}
