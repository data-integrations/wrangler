public class ByteSizeTest {

  @Test
  public void testValidByteSizes() {
    ByteSize size1 = new ByteSize("10KB");
    Assert.assertEquals(10 * 1024L, size1.getBytes());

    ByteSize size2 = new ByteSize("1.5MB");
    Assert.assertEquals((long)(1.5 * 1024 * 1024), size2.getBytes());

    ByteSize size3 = new ByteSize("2GB");
    Assert.assertEquals(2L * 1024 * 1024 * 1024, size3.getBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidByteSize() {
    new ByteSize("10XY");
  }
}
