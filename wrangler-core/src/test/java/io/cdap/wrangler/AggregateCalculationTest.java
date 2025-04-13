package com.io.cdap.wrangler;

import org.junit.Assert;
import org.junit.Test;
import java.util.*;

public class AggregateCalculationTest {

    @Test
    public void testAggregateCalculation() {
        // Sample test data (you can replace with your actual row format)
        List<Map<String, Object>> rows = new ArrayList<>();

        Map<String, Object> row1 = new HashMap<>();
        row1.put("data_transfer_size", 10485760); // 10 MB in bytes
        row1.put("response_time", 1000000000L);   // 1 sec in ns
        rows.add(row1);

        Map<String, Object> row2 = new HashMap<>();
        row2.put("data_transfer_size", 5242880); // 5 MB in bytes
        row2.put("response_time", 2000000000L);  // 2 sec in ns
        rows.add(row2);

        // Simulated recipe output
        List<Map<String, Object>> results = TestingRig.execute("your-recipe-name", rows);

        double expectedTotalSizeInMB = (10485760 + 5242880) / (1024.0 * 1024.0); // = 15 MB
        double expectedTotalTimeInSeconds = (1000000000L + 2000000000L) / 1_000_000_000.0; // = 3 sec

        Assert.assertEquals(1, results.size());
        Assert.assertEquals(expectedTotalSizeInMB, (double) results.get(0).get("total_size_mb"), 0.001);
        Assert.assertEquals(expectedTotalTimeInSeconds, (double) results.get(0).get("total_time_sec"), 0.001);
    }
}