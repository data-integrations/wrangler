import org.junit.Assert;
import org.junit.Test;
import java.util.List;
import java.util.ArrayList;

public class AggregateStatsTest {
  
  @Test
  public void testAggregation() {
    // Create sample rows with data transfer size and response time
    List<Row> rows = new ArrayList<>();
    
    Row row1 = new Row();
    row1.setColumn("data_transfer_size", 1024L); // 1KB
    row1.setColumn("response_time", 2000000000L); // 2 seconds in nanoseconds
    rows.add(row1);
    Row row2 = new Row();
    row2.setColumn("data_transfer_size", 2048L); // 2KB
    row2.setColumn("response_time", 3000000000L); // 3 seconds in nanoseconds
    rows.add(row2);

    // Define the recipe
    String[] recipe = new String[] {
      "aggregate-stats :data_transfer_size :response_time total_size_mb total_time_sec"
    };

    // Execute the recipe using TestingRig  
    List<Row> results = TestingRig.execute(recipe, rows);

    // Expected calculations
    long totalSizeBytes = 1024 + 2048; // Total size in bytes
    double totalSizeMB = totalSizeBytes / (1024.0 * 1024.0); // Convert to MB

    long totalTimeNs = 2000000000L + 3000000000L; // Total time in nanoseconds
    double totalTimeSec = totalTimeNs / 1_000_000_000.0; // Convert to seconds

    // Assertions
    Assert.assertEquals(1, results.size()); // Ensure only one row is returned
    
    // Assert the aggregated size in MB
    Assert.assertEquals(totalSizeMB, results.get(0).getValue("total_size_mb"), 0.001);

    // Assert the aggregated time in seconds
    Assert.assertEquals(totalTimeSec, results.get(0).getValue("total_time_sec"), 0.001);
  }
}