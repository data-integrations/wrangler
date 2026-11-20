package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long nanoseconds;

    public TimeDuration(String value) {
        super(value);
        this.nanoseconds = parseNanoseconds(value);
    }

    private long parseNanoseconds(String value) {
        // Parse the value and convert to nanoseconds
        String unit = value.replaceAll("[0-9.]", "").toLowerCase();
        double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
        switch (unit) {
            case "ms": return (long) (number * 1_000_000);
            case "s": return (long) (number * 1_000_000_000);
            case "m": return (long) (number * 60 * 1_000_000_000L);
            case "h": return (long) (number * 3600 * 1_000_000_000L);
            default: return (long) number; // Assume nanoseconds
        }
    }

    public long getNanoseconds() {
        return nanoseconds;
    }
}
