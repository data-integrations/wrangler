package io.cdap.wrangler.api.parser;


import org.junit.Assert;
import org.junit.Test;

public class ByteSizeTimeDurationTests {

    @Test
    public void testByteSizeParsing() {
        try {
            Assert.assertEquals(10 * 1024, ByteSize.parseByteSize("10kb"));
            Assert.assertEquals(1_048_576, ByteSize.parseByteSize("1MB"));
            Assert.assertEquals(5 * 1024 * 1024, ByteSize.parseByteSize("5MB"));
            Assert.assertEquals(10_000_000L, ByteSize.parseByteSize("10MB"));
        } catch (IllegalArgumentException e) {
            Assert.fail("Parsing failed: " + e.getMessage());
        }
    }

    @Test
    public void testInvalidByteSizeParsing() {
        try {
            ByteSize.parseByteSize("invalidSize");
            Assert.fail("Expected an exception due to invalid byte size");
        } catch (IllegalArgumentException e) {
            Assert.fail("Parsing failed: " + e.getMessage());
        }
    }

    @Test
    public void testTimeDurationParsing() {
        try {
            Assert.assertEquals(5000L, TimeDuration.parseTimeDuration("5ms"));
            Assert.assertEquals(1_000_000_000L, TimeDuration.parseTimeDuration("1s"));
            Assert.assertEquals(2_100_000_000L, TimeDuration.parseTimeDuration("2.1s"));
            Assert.assertEquals(3_600_000_000_000L, TimeDuration.parseTimeDuration("1h"));
        } catch (IllegalArgumentException e) {
            Assert.fail("Parsing failed: " + e.getMessage());
        }
    }

    @Test
    public void testInvalidTimeDurationParsing() {
        try {
            TimeDuration.parseTimeDuration("invalidDuration");
            Assert.fail("Expected an exception due to invalid time duration");
        } catch (IllegalArgumentException e) {
            Assert.fail("Parsing failed: " + e.getMessage());
        }
    }
}

