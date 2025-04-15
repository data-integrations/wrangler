public class AggregateStatsTest {
    @Test
    public void testAggregateStats() throws Exception {
        List<Row> rows = Arrays.asList(
            new Row("data_transfer", "10KB").add("response_time", "2s"),
            new Row("data_transfer", "1.5MB").add("response_time", "3s")
        );

        String[] recipe = {
            "aggregate-stats :data_transfer :response_time total_size_mb total_time_sec"
        };

        List<Row> results = TestingRig.execute(recipe, rows);

        double expectedMB = (10240 + 1572864) / (1024.0 * 1024);
        double expectedSec = (2000 + 3000) / 1000.0;

        Assert.assertEquals(expectedMB, results.get(0).getValue("total_size_mb"), 0.01);
        Assert.assertEquals(expectedSec, results.get(0).getValue("total_time_sec"), 0.01);
    }
}
