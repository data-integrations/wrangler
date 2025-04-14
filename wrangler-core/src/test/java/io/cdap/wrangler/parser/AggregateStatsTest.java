@Test  
public void testAggregation() {  
  List<Row> input = Arrays.asList(  
    new Row("size", "10KB", "time", "500ms"),  
    new Row("size", "2MB", "time", "1s")  
  );  

  String[] recipe = {"aggregate-stats :size :time total_size total_time"};  
  List<Row> results = TestingRig.execute(recipe, input);  

  assertEquals(1, results.size());  
  assertEquals(10*1024 + 2*1024*1024, results.get(0).getValue("total_size"));  
  assertEquals(500_000_000L + 1_000_000_000L, results.get(0).getValue("total_time"));  
}  