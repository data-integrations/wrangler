public class AggregateStats implements Directive {
    // Fields to store column names and totals
    // Implement initialize(), define(), execute(), and finalize()

    @Override
    public UsageDefinition define() {
        return UsageDefinition.builder("aggregate-stats")
            .define("inputByteSizeColumn", TokenType.COLUMN_NAME)
            .define("inputTimeColumn", TokenType.COLUMN_NAME)
            .define("outputSizeColumn", TokenType.COLUMN_NAME)
            .define("outputTimeColumn", TokenType.COLUMN_NAME)
            .build();
    }

    @Override
    public void initialize(Arguments args) {
        // Read and store column names from args
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context) {
        // For each row, read values, convert to bytes/ms, add to totals
        // Return a single row with calculated totals
    }
}
