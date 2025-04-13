@Test
public void testAggregateStats() {
  List<Row> rows = Arrays.asList(
    new Row().add("size", "1MB").add("time", "1s"),
    new Row().add("size", "2MB").add("time", "2s")
  );

  String[] recipe = {
    "aggregate-stats :size :time total_size_mb total_time_sec"
  };

  List<Row> results = TestingRig.execute(recipe, rows);
  assertEquals(1, results.size());
  assertEquals(3.0, results.get(0).getValue("total_size_mb"));
  assertEquals(3.0, results.get(0).getValue("total_time_sec"));
}
