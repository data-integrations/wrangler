package io.cdap.wrangler.parser;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.Directive;
import io.cdap.wrangler.api.parser.TokenGroup;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.List;

/**
 * Example usage:
 * aggregate-stats :data_transfer :response_time total_size_mb total_time_sec
 */
public class AggregateStats implements Directive {
  private String sizeCol;
  private String timeCol;
  private String outSizeCol;
  private String outTimeCol;

  private long totalBytes = 0;
  private long totalTimeMs = 0;
  private int rowCount = 0;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats")
      .addRequiredArg("source_size_column", ColumnName.class)
      .addRequiredArg("source_time_column", ColumnName.class)
      .addRequiredArg("target_size_column", ColumnName.class)
      .addRequiredArg("target_time_column", ColumnName.class)
      .build();
  }

  @Override
  public void initialize(DirectiveContext ctx, TokenGroup args) {
    sizeCol = ((ColumnName) args.get(0)).value();
    timeCol = ((ColumnName) args.get(1)).value();
    outSizeCol = ((ColumnName) args.get(2)).value();
    outTimeCol = ((ColumnName) args.get(3)).value();
  }

  @Override
  public List<Row> execute(DirectiveContext ctx, List<Row> rows) {
    for (Row row : rows) {
      Object sizeVal = row.getValue(sizeCol);
      Object timeVal = row.getValue(timeCol);

      long bytes = 0;
      long millis = 0;

      try {
        if (sizeVal != null) {
          ByteSize size = new ByteSize(sizeVal.toString());
          bytes = size.getBytes();
        }

        if (timeVal != null) {
          TimeDuration duration = new TimeDuration(timeVal.toString());
          millis = duration.getMilliseconds();
        }
      } catch (Exception e) {
        throw new RuntimeException("Error parsing row values: " + row.toString(), e);
      }

      totalBytes += bytes;
      totalTimeMs += millis;
      rowCount++;
    }

    double totalMB = totalBytes / (1024.0 * 1024.0); // MB
    double totalSeconds = totalTimeMs / 1000.0; // seconds

    Row result = new Row();
    result.add(outSizeCol, totalMB);
    result.add(outTimeCol, totalSeconds);

    return List.of(result);
  }
}
