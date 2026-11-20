package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.*;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.ArrayList;
import java.util.List;

@Plugin(type = Directive.TYPE)
@Name("aggregate-stats")
@Description("Aggregates byte size and time duration columns")
public class AggregateStats implements Directive, AggregateInterpreter {
  private String sizeColumn;
  private String timeColumn;
  private String outputSizeColumn;
  private String outputTimeColumn;
  private long totalBytes = 0;
  private long totalNanos = 0;
  private int rowCount = 0;

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder("aggregate-stats");
    builder.define("size-column", TokenType.COLUMN_NAME);
    builder.define("time-column", TokenType.COLUMN_NAME);
    builder.define("output-size-column", TokenType.COLUMN_NAME);
    builder.define("output-time-column", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.sizeColumn = ((ColumnName) args.value("size-column")).value();
    this.timeColumn = ((ColumnName) args.value("time-column")).value();
    this.outputSizeColumn = ((ColumnName) args.value("output-size-column")).value();
    this.outputTimeColumn = ((ColumnName) args.value("output-time-column")).value();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    for (Row row : rows) {
      try {
        // Process byte size
        Object sizeObj = row.getValue(sizeColumn);
        if (sizeObj != null) {
          ByteSize byteSize = new ByteSize(sizeObj.toString());
          totalBytes += byteSize.getBytes();
        }

        // Process time duration
        Object timeObj = row.getValue(timeColumn);
        if (timeObj != null) {
          TimeDuration timeDuration = new TimeDuration(timeObj.toString());
          totalNanos += timeDuration.getNanoseconds();
        }

        rowCount++;
      } catch (Exception e) {
        throw new DirectiveExecutionException(e.getMessage(), e);
      }
    }
    return rows;
  }

  @Override
  public List<Row> finalize() throws DirectiveExecutionException {
    Row row = new Row();
    row.add(outputSizeColumn, (double) totalBytes / (1024 * 1024)); // Convert to MB
    row.add(outputTimeColumn, (double) totalNanos / 1_000_000_000); // Convert to seconds
    List<Row> result = new ArrayList<>();
    result.add(row);
    return result;
  }

  @Override
  public void destroy() {
    // Clean up if needed
  }
}

