public class ByteSizeTest {
    @Test
    public void testParsing() {
        assertEquals(1024, new ByteSize("1KB").getBytes());
        assertEquals(1.0, new ByteSize("1024KB").getMB(), 0.001);
    }
}