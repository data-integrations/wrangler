package io.cdap.wrangler.aggregate;

import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.ColumnName;

import com.google.gson.JsonObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateStatsTest {
  public static void main(String[] args) throws Exception {
    // Create test rows
    Row row1 = new Row();
    row1.add("data_transfer_size", "10KB");
    row1.add("response_time", "500ms");

    Row row2 = new Row();
    row2.add("data_transfer_size", "1MB");
    row2.add("response_time", "2s");

    List<Row> rows = Arrays.asList(row1, row2);

    // Mock Arguments
    Map<String, ColumnName> argumentMap = new HashMap<>();
    argumentMap.put("sizeColumn", new ColumnName("data_transfer_size"));
    argumentMap.put("timeColumn", new ColumnName("response_time"));
    argumentMap.put("totalSizeColumn", new ColumnName("total_size_mb"));
    argumentMap.put("totalTimeColumn", new ColumnName("total_time_sec"));

    Arguments arguments = new Arguments() {
      @Override
      public ColumnName value(String name) {
        return argumentMap.get(name);
      }

      @Override
      public boolean contains(String name) {
        return argumentMap.containsKey(name);
      }

      @Override
      public JsonObject toJson() {
        return new JsonObject();
      }

      @Override
      public int column() {
        return 0;
      }

      @Override
      public int line() {
        return 0;
      }

      @Override
      public int size() {
        return argumentMap.size();
      }

      @Override
      public TokenType type(String name) {
        return TokenType.COLUMN_NAME;
      }

      @Override
      public String source() {
        return "";
      }
    };

    ExecutorContext context = null;

    AggregateStats directive = new AggregateStats();
    directive.initialize(arguments);
    List<Row> result = directive.execute(rows, context);

    Row output = result.get(0);
    System.out.println("Total Size (MB): " + output.getValue("total_size_mb"));
    System.out.println("Total Time (Sec): " + output.getValue("total_time_sec"));
  }
}

