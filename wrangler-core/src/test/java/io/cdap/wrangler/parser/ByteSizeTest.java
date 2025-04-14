@Test  
public void testMBConversion() {  
  ByteSize bs = new ByteSize("5MB");  
  assertEquals(5 * 1024 * 1024, bs.getBytes());  
}  
