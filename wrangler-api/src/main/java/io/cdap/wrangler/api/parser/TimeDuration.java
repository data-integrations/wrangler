// TimeDuration.java
package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long nanos;
    
    public TimeDuration(String value) {
        super(TokenType.TIME_DURATION);
        this.nanos = parseNanos(value);
    }

    private long parseNanos(String value) {
        String num = value.replaceAll("[^0-9]", "");
        String unit = value.replaceAll("[0-9]", "").toLowerCase();
        
        long multiplier = 1;
        switch (unit) {
            case "ms": multiplier = 1_000_000; break;
            case "s": multiplier = 1_000_000_000; break;
            case "m": multiplier = 60L * 1_000_000_000; break;
            case "h": multiplier = 3600L * 1_000_000_000; break;
        }
        
        return Long.parseLong(num) * multiplier;
    }

    public long getNanos() { return nanos; }
    public long getMillis() { return nanos / 1_000_000; }
    public double getSeconds() { return nanos / 1_000_000_000.0; }
}