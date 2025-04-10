package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long milliseconds;

    public TimeDuration(String value) {
        super(value);
        this.milliseconds = parseMilliseconds(value);
    }

    private long parseMilliseconds(String str) {
        str = str.trim().toLowerCase();
        double number = Double.parseDouble(str.replaceAll("[^0-9.]", ""));
        if (str.endsWith("ms")) return (long)(number);
        if (str.endsWith("s")) return (long)(number * 1000);
        if (str.endsWith("m")) return (long)(number * 60 * 1000);
        if (str.endsWith("h")) return (long)(number * 60 * 60 * 1000);
        if (str.endsWith("d")) return (long)(number * 24 * 60 * 60 * 1000);
        return (long) number; // Default to milliseconds
    }

    public long getMilliseconds() {
        return milliseconds;
    }
}
