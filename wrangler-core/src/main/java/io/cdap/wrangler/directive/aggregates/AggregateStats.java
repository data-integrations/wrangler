package io.cdap.wrangler.directive.aggregates;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ColumnName;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveAggregate;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;

import java.util.Collections;
import java.util.List;

/**
 * Directive to compute aggregate statistics from byte size and time duration columns.
 */
public class AggregateStats implements Directive, DirectiveAggregate {
  private String sizeCol;
  private String timeCol;
  private String outputSizeCol;
  private String outputTimeCol;

  private long totalBytes;
  private long totalMillis;
  private int count;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats")
      .define("sizeCol", TokenType.COLUMN_NAME)
      .define("timeCol", TokenType.COLUMN_NAME)
      .define("outputSizeCol", TokenType.COLUMN_NAME)
      .define("outputTimeCol", TokenType.COLUMN_NAME)
      .build();
  }

  @Override
  public void initialize(Arguments args) {
    this.sizeCol = ((ColumnName) args.value("sizeCol")).value();
    this.timeCol = ((ColumnName) args.value("timeCol")).value();
    this.outputSizeCol = ((ColumnName) args.value("outputSizeCol")).value();
    this.outputTimeCol = ((ColumnName) args.value("outputTimeCol")).value();

    this.totalBytes = 0;
    this.totalMillis = 0;
    this.count = 0;
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) {
    for (Row row : rows) {
      Object sizeObj = row.getValue(sizeCol);
      Object timeObj = row.getValue(timeCol);

      if (sizeObj != null && timeObj != null) {
        try {
          long bytes = new ByteSize(sizeObj.toString()).getBytes();
          long millis = new TimeDuration(timeObj.toString()).getMillis();

          totalBytes += bytes;
          totalMillis += millis;
          count++;
        } catch (Exception e) {
          // You can log here or skip invalid data silently
        }
      }
    }

    double totalSizeMb = totalBytes / (1024.0 * 1024.0);
    double totalTimeSec = totalMillis / 1000.0;

    Row result = new Row();
    result.add(outputSizeCol, totalSizeMb);
    result.add(outputTimeCol, totalTimeSec);

    return Collections.singletonList(result);
  }

  @Override
  public void destroy() {
    // No cleanup necessary
  }
}
