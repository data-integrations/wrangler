public class AggregateStats implements Directive {
    private String sizeCol, timeCol, outputSizeCol, outputTimeCol;

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .define("sizeCol", TokenType.COLUMN_NAME)
            .define("timeCol", TokenType.COLUMN_NAME)
            .define("outputSizeCol", TokenType.STRING)
            .define("outputTimeCol", TokenType.STRING)
            .build();
    }

    @Override
    public void initialize(Arguments args) {
        sizeCol = ((ColumnName) args.value("sizeCol")).value();
        timeCol = ((ColumnName) args.value("timeCol")).value();
        outputSizeCol = ((Text) args.value("outputSizeCol")).value();
        outputTimeCol = ((Text) args.value("outputTimeCol")).value();
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext ctx) {
        long totalBytes = 0;
        long totalMillis = 0;

        for (Row row : rows) {
            String byteVal = row.getValue(sizeCol).toString();
            String timeVal = row.getValue(timeCol).toString();
            totalBytes += new ByteSize(byteVal).getBytes();
            totalMillis += new TimeDuration(timeVal).getMillis();
        }

        Row result = new Row();
        result.add(outputSizeCol, totalBytes / (1024.0 * 1024)); // MB
        result.add(outputTimeCol, totalMillis / 1000.0); // seconds

        return Collections.singletonList(result);
    }
}
