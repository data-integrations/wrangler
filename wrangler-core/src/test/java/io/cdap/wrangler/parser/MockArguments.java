class MockArguments implements Arguments {
    private final Map<String, Object> args = new HashMap<>();

    public MockArguments(String... values) {
        args.put("size_column", new ColumnName(values[0]));
        args.put("time_column", new ColumnName(values[1]));
        args.put("output_size_column", new ColumnName(values[2]));
        args.put("output_time_column", new ColumnName(values[3]));

        if (values.length > 4) args.put("output_size_unit", values[4]);
        if (values.length > 5) args.put("output_time_unit", values[5]);
        if (values.length > 6) args.put("aggregation_type", values[6]);
    }

    @Override public boolean contains(String key) { return args.containsKey(key); }
    @Override public Object value(String key) { return args.get(key); }
}