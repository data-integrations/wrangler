package io.cdap.directives.aggregates;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.TransientStore;
import io.cdap.wrangler.api.TransientVariableScope;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.parser.ColumnName;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.api.parser.ByteSize;
import io.cdap.wrangler.api.parser.TimeDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Directive that aggregates byte size and time duration values.
 */
@Plugin(type = Directive.TYPE)
@Name(AggregateStats.NAME)
@Categories(categories = {"aggregates"})
@Description("Aggregates byte size and time duration values into total statistics")
public class AggregateStats implements Directive {
  public static final String NAME = "aggregate-stats";
  private String byteSizeColumn;
  private String timeDurationColumn;
  private String totalSizeColumn;
  private String totalTimeColumn;

  /**
   * Class to store aggregated values.
   */
  private static class AggregationValues implements Serializable {
    private static final long serialVersionUID = 1L;
    long totalBytes = 0;
    double totalSeconds = 0.0;
  }

  @Override
  public UsageDefinition define() {
    UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
    builder.define("byteSizeColumn", TokenType.COLUMN_NAME);
    builder.define("timeDurationColumn", TokenType.COLUMN_NAME);
    builder.define("totalSizeColumn", TokenType.COLUMN_NAME);
    builder.define("totalTimeColumn", TokenType.COLUMN_NAME);
    return builder.build();
  }

  @Override
  public void initialize(Arguments args) throws DirectiveParseException {
    this.byteSizeColumn = ((ColumnName) args.value("byteSizeColumn")).value();
    this.timeDurationColumn = ((ColumnName) args.value("timeDurationColumn")).value();
    this.totalSizeColumn = ((ColumnName) args.value("totalSizeColumn")).value();
    this.totalTimeColumn = ((ColumnName) args.value("totalTimeColumn")).value();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws DirectiveExecutionException {
    // Get the transient store from the context.
    TransientStore store = context.getTransientStore();

    // Create a unique key for storing our aggregate values.
    String storeKey = NAME + "-" + byteSizeColumn + "-" + timeDurationColumn;

    // Retrieve the current aggregation values from the store.
    AggregationValues aggValues = (AggregationValues) store.get(storeKey);

    // If no values exist yet for this key, initialize them.
    if (aggValues == null) {
      aggValues = new AggregationValues();
    }

    // Process each row in the batch.
    for (Row row : rows) {
      // Process the byte size column.
      int byteSizeIdx = row.find(byteSizeColumn);
      if (byteSizeIdx != -1) {
        Object byteSizeObj = row.getValue(byteSizeColumn);
        if (byteSizeObj != null) {
          try {
            long bytes;
            if (byteSizeObj instanceof ByteSize) {
              bytes = ((ByteSize) byteSizeObj).getBytes();
            } else {
              // Try to parse as string.
              bytes = new ByteSize(byteSizeObj.toString()).getBytes();
            }
            aggValues.totalBytes += bytes;
          } catch (Exception e) {
            // Optionally log or handle invalid values.
          }
        }
      }

      // Process the time duration column.
      int timeDurationIdx = row.find(timeDurationColumn);
      if (timeDurationIdx != -1) {
        Object timeDurationObj = row.getValue(timeDurationColumn);
        if (timeDurationObj != null) {
          try {
            double seconds;
            if (timeDurationObj instanceof TimeDuration) {
              // Assume getMillis() returns time in milliseconds.
              seconds = ((TimeDuration) timeDurationObj).getMillis() / 1000.0;
            } else {
              seconds = new TimeDuration(timeDurationObj.toString()).getMillis() / 1000.0;
            }
            aggValues.totalSeconds += seconds;
          } catch (Exception e) {
            // Optionally log or handle invalid values.
          }
        }
      }
    }

    // Update the values in the store (set() requires scope).
    store.set(TransientVariableScope.LOCAL, storeKey, aggValues);

    // Check if this is the final batch.
    boolean isFinalBatch = store.get("batch.final") != null;

    if (isFinalBatch) {
      // Finalization: retrieve the aggregated totals.
      AggregationValues finalValues = (AggregationValues) store.get(storeKey);

      // Convert bytes to megabytes (MB).
      double totalSizeMB = finalValues.totalBytes / (1024.0 * 1024.0);

      // Create a new result row with the target columns.
      Row resultRow = new Row();
      resultRow.add(totalSizeColumn, totalSizeMB);
      resultRow.add(totalTimeColumn, finalValues.totalSeconds); // total time in seconds.

      // Clean up: remove the aggregate values from the store.
      store.set(TransientVariableScope.LOCAL, storeKey, null);  

      // Return the result row.
      List<Row> results = new ArrayList<>();
      results.add(resultRow);
      return results;
    }

    // For non-final batches, return the input rows unchanged.
    return rows;
  }

  @Override
  public void destroy() {
    // No resources to clean up.
  }
}
