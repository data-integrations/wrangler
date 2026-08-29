import org.junit.Test;
import static org.junit.Assert.*;

public class TimeDurationTest {

    @Test
    public void testParsing() {
        TimeDuration time1 = new TimeDuration("5ms");
        assertEquals(5, time1.getMilliseconds());

        TimeDuration time2 = new TimeDuration("2s");
        assertEquals(2000, time2.getMilliseconds());

        TimeDuration time3 = new TimeDuration("3.5m");
        assertEquals(3.5 * 60 * 1000, time3.getMilliseconds(), 0.01);

        TimeDuration time4 = new TimeDuration("1h");
        assertEquals(1L * 60 * 60 * 1000, time4.getMilliseconds());

        TimeDuration time5 = new TimeDuration("2d");
        assertEquals(2L * 24 * 60 * 60 * 1000, time5.getMilliseconds());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidUnit() {
        new TimeDuration("100XYZ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMalformedInput() {
        new TimeDuration("3.5abc");
    }
}
