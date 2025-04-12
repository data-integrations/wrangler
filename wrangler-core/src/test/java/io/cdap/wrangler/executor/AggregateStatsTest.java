public class AggregateStatsTest {

  @Test
  public void testAggregateStats() throws Exception {
    List<Row> rows = Arrays.asList(
      new Row("data_transfer_size", "1MB").add("response_time", "1s"),
      new Row("data_transfer_size", "512KB").add("response_time", "500ms")
    );

    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    List<Row> results = TestingRig.execute(recipe, rows);
    Assert.assertEquals(1, results.size());

    double totalMB = (1 * 1024 * 1024 + 512 * 1024) / (1024.0 * 1024.0);
    double totalSec = (1_000 + 500) / 1000.0;

    Assert.assertEquals(totalMB, results.get(0).getValue("total_size_mb"));
    Assert.assertEquals(totalSec, results.get(0).getValue("total_time_sec"));
  }
}
