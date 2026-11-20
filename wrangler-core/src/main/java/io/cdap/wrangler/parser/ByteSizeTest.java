public class ByteSizeTest {
  @Test
  public void testParsing() {
    ByteSize bs = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024, bs.getBytes());

    bs = new ByteSize("1.5MB");
    Assert.assertEquals((long)(1.5 * 1024 * 1024), bs.getBytes());
  }
}
