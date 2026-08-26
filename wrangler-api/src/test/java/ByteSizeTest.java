@Test
public void testByteSize() {
  assertEquals(10240, new ByteSize("10KB").getBytes());
}
