package io.cdap.directives.aggregates;

import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.executor.*;

import java.util.List;

@PublicEvolving
public class AggregateStats implements Directive, AggregateInterpreter {
  public static final String NAME = "aggregate-stats";
  private String sizeColumn;
  private String timeColumn;
  private String outputSizeColumn;
  private String outputTimeColumn;
  private String sizeUnit = "MB";
  private String timeUnit = "S";
  private String aggregationType = "total";

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("size-column", TokenType.COLUMN_NAME);
    builder.define("time-column", TokenType.COLUMN_NAME);
    builder.define("output-size-column", TokenType.COLUMN_NAME);
    builder.define("output-time-column", TokenType.COLUMN_NAME);
    builder.define("size-unit", TokenType.STRING, true);
    builder.define("time-unit", TokenType.STRING, true);
    builder.define("aggregation-type", TokenType.STRING, true);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((ColumnName) args.value("size-column")).value();
    this.timeColumn = ((ColumnName) args.value("time-column")).value();
    this.outputSizeColumn = ((ColumnName) args.value("output-size-column")).value();
    this.outputTimeColumn = ((ColumnName) args.value("output-time-column")).value();
    
    if (args.contains("size-unit")) {
      this.sizeUnit = ((Text) args.value("size-unit")).value();
    }
    if (args.contains("time-unit")) {
      this.timeUnit = ((Text) args.value("time-unit")).value();
    }
    if (args.contains("aggregation-type")) {
      this.aggregationType = ((Text) args.value("aggregation-type")).value();
    }
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    Stores stores = context.getStores();
    String sizeKey = String.format("%s::%s::size", NAME, sizeColumn);
    String timeKey = String.format("%s::%s::time", NAME, timeColumn);
    String countKey = String.format("%s::%s::count", NAME, sizeColumn);

    for (Row row : rows) {
      // Process size
      Object sizeObj = row.getValue(sizeColumn);
      if (sizeObj != null) {
        ByteSize size = new ByteSize(sizeObj.toString());
        stores.increment(sizeKey, size.getBytes());
      }

      // Process time
      Object timeObj = row.getValue(timeColumn);
      if (timeObj != null) {
        TimeDuration time = new TimeDuration(timeObj.toString());
        stores.increment(timeKey, time.getNanoseconds());
      }

      stores.increment(countKey, 1L);
    }

    return rows;
  }

  @Override
  public List<Row> finalize(ExecutorContext context) throws DirectiveExecutionException {
    Stores stores = context.getStores();
    String sizeKey = String.format("%s::%s::size", NAME, sizeColumn);
    String timeKey = String.format("%s::%s::time", NAME, timeColumn);
    String countKey = String.format("%s::%s::count", NAME, sizeColumn);

    long totalSize = stores.getLong(sizeKey, 0L);
    long totalTime = stores.getLong(timeKey, 0L);
    long count = stores.getLong(countKey, 0L);

    double outputSize = new ByteSize(totalSize + "B").getBytesAs(sizeUnit);
    double outputTime = new TimeDuration(totalTime + "NS").getDurationAs(timeUnit);

    if ("average".equalsIgnoreCase(aggregationType) && count > 0) {
      outputSize = outputSize / count;
      outputTime = outputTime / count;
    }

    Row result = new Row();
    result.add(outputSizeColumn, outputSize);
    result.add(outputTimeColumn, outputTime);

    return Collections.singletonList(result);
  }

  @Override
  public void destroy() {
    // No-op
  }
}