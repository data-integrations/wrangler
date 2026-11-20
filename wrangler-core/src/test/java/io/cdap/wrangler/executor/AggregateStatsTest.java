

 package io.cdap.wrangler.executor;

 import io.cdap.wrangler.TestingRig;
 import io.cdap.wrangler.api.Row;
 import io.cdap.wrangler.api.parser.ByteSize;
 import io.cdap.wrangler.api.parser.TimeDuration;
 import org.junit.Assert;
 import org.junit.Test;
 
 import java.util.Arrays;
 import java.util.List;
 
 public class AggregateStatsTest {
   @Test
   public void testBasicAggregation() throws Exception {
     String[] directives = new String[] {
       "aggregate-stats :data_size :response_time :total_size :total_time"
     };
 
     List<Row> rows = Arrays.asList(
       createRow("1KB", "100ms"),
       createRow("2MB", "500ms"),
       createRow("0.5GB", "2s"),
       createRow("1.5MB", "1.5s")
     );
 
     List<Row> results = TestingRig.execute(directives, rows);
     Assert.assertEquals(1, results.size());
     
     Row result = results.get(0);
     Assert.assertEquals("0.50GB", result.getValue("total_size"));
     Assert.assertEquals("4.10s", result.getValue("total_time"));
   }
 
   @Test
   public void testMixedFormats() throws Exception {
     String[] directives = new String[] {
       "aggregate-stats :data_size :response_time :total_size :total_time"
     };
 
     List<Row> rows = Arrays.asList(
       createRow(new ByteSize("1KB"), new TimeDuration("100ms")),
       createRow("2MB", "500ms"),
       createRow(new ByteSize("0.5GB"), new TimeDuration("2s")),
       createRow("1.5MB", "1.5s")
     );
 
     List<Row> results = TestingRig.execute(directives, rows);
     Assert.assertEquals(1, results.size());
     
     Row result = results.get(0);
     Assert.assertEquals("0.50GB", result.getValue("total_size"));
     Assert.assertEquals("4.10s", result.getValue("total_time"));
   }
 
   @Test
   public void testEdgeCases() throws Exception {
     String[] directives = new String[] {
       "aggregate-stats :data_size :response_time :total_size :total_time"
     };
 
     List<Row> rows = Arrays.asList(
       createRow("0KB", "0ms"),
       createRow("1PB", "1ns"),
       createRow("0.001KB", "0.001ms")
     );
 
     List<Row> results = TestingRig.execute(directives, rows);
     Assert.assertEquals(1, results.size());
     
     Row result = results.get(0);
     Assert.assertEquals("1.00PB", result.getValue("total_size"));
     Assert.assertEquals("0.00s", result.getValue("total_time"));
   }
 
   @Test(expected = Exception.class)
   public void testInvalidSizeFormat() throws Exception {
     String[] directives = new String[] {
       "aggregate-stats :data_size :response_time :total_size :total_time"
     };
 
     List<Row> rows = Arrays.asList(
       createRow("invalid", "100ms")
     );
 
     TestingRig.execute(directives, rows);
   }
 
   @Test(expected = Exception.class)
   public void testInvalidTimeFormat() throws Exception {
     String[] directives = new String[] {
       "aggregate-stats :data_size :response_time :total_size :total_time"
     };
 
     List<Row> rows = Arrays.asList(
       createRow("1KB", "invalid")
     );
 
     TestingRig.execute(directives, rows);
   }
 
   private Row createRow(Object size, Object time) {
     Row row = new Row();
     row.add("data_size", size);
     row.add("response_time", time);
     return row;
   }
 }
 
