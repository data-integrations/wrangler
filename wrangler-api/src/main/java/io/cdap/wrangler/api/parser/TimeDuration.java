package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long nanoseconds;

    public TimeDuration(String value) {
        super(value);
        this.nanoseconds = parseNanoseconds(value);
    }

    private long parseNanoseconds(String value) {
        // ...logic to parse "150ms", "2.1s", etc., and convert to nanoseconds...
    }

    public long getNanoseconds() {
        return nanoseconds;
    }
}
