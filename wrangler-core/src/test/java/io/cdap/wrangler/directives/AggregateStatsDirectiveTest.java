
// AggregateStatsDirectiveTest.java
public class AggregateStatsDirectiveTest {
    @Test
    public void testAggregation() throws Exception {
        List<Row> rows = Arrays.asList(
            new Row("data_size", "1MB", "response_time", "100ms"),
            new Row("data_size", "2MB", "response_time", "200ms")
        );
        
        String[] recipe = {
            "aggregate-stats :data_size :response_time :total_size_mb :total_time_sec"
        };
        
        List<Row> results = TestingRig.execute(recipe, rows);
        assertEquals(3.0, (Double)results.get(0).getValue("total_size_mb"), 0.001);
        assertEquals(0.3, (Double)results.get(0).getValue("total_time_sec"), 0.001);
    }
}