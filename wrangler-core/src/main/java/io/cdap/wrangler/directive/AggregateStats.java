public class AggregateStats implements Directive {
  private String sizeCol, timeCol, outSizeCol, outTimeCol;
  private long totalBytes = 0, totalNanos = 0;
  private int rowCount = 0;

  @Override
  public void initialize(Arguments args) {
      sizeCol = args.value("size_col");
      timeCol = args.value("time_col");
      outSizeCol = args.value("out_size_col");
      outTimeCol = args.value("out_time_col");
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext ctx) {
      for (Row row : rows) {
          String sizeVal = row.getValue(sizeCol).toString();
          String timeVal = row.getValue(timeCol).toString();
          totalBytes += new ByteSize(sizeVal).getBytes();
          totalNanos += new TimeDuration(timeVal).getNanos();
          rowCount++;
      }
      List<Row> output = new ArrayList<>();
      Row result = new Row();
      result.add(outSizeCol, totalBytes / (1024.0 * 1024.0)); // to MB
      result.add(outTimeCol, totalNanos / 1_000_000_000.0); // to seconds
      output.add(result);
      return output;
  }

  ...
}
