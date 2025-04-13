package io.cdap.wrangler;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.annotations.Name;
import io.cdap.wrangler.api.annotations.Description;

import java.util.List;
import java.util.ArrayList;

@Name("aggregate-stats")
@Description("Aggregates byte size and time duration into MB and seconds.")
public class AggregateStats implements Directive {
  private String sizeCol;
  private String timeCol;
  private String outSizeCol;
  private String outTimeCol;

  private long totalBytes = 0;
  private long totalMillis = 0;

  public void initialize(DirectiveContext ctx, String[] args) {
    this.sizeCol = args[0];
    this.timeCol = args[1];
    this.outSizeCol = args[2];
    this.outTimeCol = args[3];
  }

  public List<Row> execute(List<Row> rows) throws DirectiveExecutionException {
    for (Row row : rows) {
      String sizeStr = row.getValue(sizeCol).toString();
      String timeStr = row.getValue(timeCol).toString();

      totalBytes += new ByteSize(sizeStr).getBytes();
      totalMillis += new TimeDuration(timeStr).getMilliseconds();
    }

    List<Row> out = new ArrayList<>();
    Row outRow = new Row();
    outRow.add(outSizeCol, totalBytes / (1024.0 * 1024.0));  // Convert to MB
    outRow.add(outTimeCol, totalMillis / 1000.0);  // Convert to seconds
    out.add(outRow);
    return out;
  }

  public void destroy() {}
}