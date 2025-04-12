public class TimeDurationTest {

  @Test
  public void testValidDurations() {
    TimeDuration d1 = new TimeDuration("150ms");
    Assert.assertEquals(150_000_000L, d1.getNanos());

    TimeDuration d2 = new TimeDuration("2s");
    Assert.assertEquals(2_000_000_000L, d2.getNanos());

    TimeDuration d3 = new TimeDuration("1.25m");
    Assert.assertEquals((long)(1.25 * 60 * 1_000_000_000L), d3.getNanos());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidTimeDuration() {
    new TimeDuration("123abc");
  }
}
