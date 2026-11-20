@Test
public void testAggregateStats() throws Exception {
    List<Row> input = Arrays.asList(
        new Row("size", "1MB").add("time", "200ms"),
        new Row("size", "512KB").add("time", "800ms")
    );

    String[] recipe = new String[] {
        "aggregate-stats :size :time total_size_mb total_time_sec"
    };

    List<Row> result = TestingRig.execute(recipe, input);
    Assert.assertEquals(1, result.size());

    double expectedSizeMB = (1024 * 1024 + 512 * 1024) / (1024.0 * 1024);
    double expectedTimeSec = (200 + 800) / 1000.0;

    Assert.assertEquals(expectedSizeMB, result.get(0).getValue("total_size_mb"), 0.001);
    Assert.assertEquals(expectedTimeSec, result.get(0).getValue("total_time_sec"), 0.001);
}
