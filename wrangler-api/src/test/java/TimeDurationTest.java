@Test
public void testTimeDuration() {
  assertEquals(5000, new TimeDuration("5s").getMillis());
}
