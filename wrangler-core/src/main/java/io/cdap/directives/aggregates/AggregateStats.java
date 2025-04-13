public class AggregateStats implements Directive {
  private String sizeCol, timeCol, outSizeCol, outTimeCol;
  private long totalBytes = 0, totalMillis = 0;

  @Override
  public UsageDefinition define() {
    return UsageDefinition.builder("aggregate-stats")
      .define("sizeCol", TokenType.COLUMN_NAME)
      .define("timeCol", TokenType.COLUMN_NAME)
      .define("outSizeCol", TokenType.STRING)
      .define("outTimeCol", TokenType.STRING)
      .build();
  }

  @Override
  public void initialize(Arguments args) {
    sizeCol = args.value("sizeCol");
    timeCol = args.value("timeCol");
    outSizeCol = args.value("outSizeCol");
    outTimeCol = args.value("outTimeCol");
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext ctx) {
    for (Row row : rows) {
      totalBytes += new ByteSize(row.getValue(sizeCol).toString()).getBytes();
      totalMillis += new TimeDuration(row.getValue(timeCol).toString()).getMillis();
    }
    Row result = new Row();
    result.add(outSizeCol, totalBytes / (1024.0 * 1024)); // MB
    result.add(outTimeCol, totalMillis / 1000.0); // seconds
    return Collections.singletonList(result);
  }
}
