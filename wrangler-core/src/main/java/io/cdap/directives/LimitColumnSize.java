package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.api.annotations.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Plugin(type = Directive.TYPE)
@Name("limit-column-size")
@Description("Removes rows where column size exceeds specified byte limit (e.g., 5KB)")
public class LimitColumnSize implements Directive {
  private String column;
  private long limit;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("limit-column-size")
      .define(TokenDefinition.builder("column").type(Type.IDENTIFIER).usage("Column to check").build())
      .define(TokenDefinition.builder("limit").type(Type.BYTE_SIZE).usage("Byte size limit").build())
      .build();
  }

  @Override
  public void initialize(Arguments arguments) throws Exception {
    this.column = arguments.value("column");
    ByteSize byteSize = arguments.value("limit");
    this.limit = byteSize.getBytes();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws Exception {
    List<Row> results = new ArrayList<>();
    for (Row row : rows) {
      Object val = row.getValue(column);
      if (val == null) continue;

      int size = val.toString().getBytes(StandardCharsets.UTF_8).length;
      if (size <= limit) {
        results.add(row);
      }
    }
    return results;
  }
}
