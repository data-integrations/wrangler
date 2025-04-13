package io.cdap.wrangler.api.parser;

/**
 * A class to parse time duration strings like "500ms", "2s", "3.5m", "1h", etc.
 */
public class TimeDuration {
    private final long milliseconds;

    public TimeDuration(String value) {
        this.milliseconds = parseTime(value.trim());
    }

    private long parseTime(String value) {
        value = value.toLowerCase();

        if (value.endsWith("ms")) {
            return (long) Double.parseDouble(value.replace("ms", ""));
        } else if (value.endsWith("s")) {
            return (long) (Double.parseDouble(value.replace("s", "")) * 1000);
        } else if (value.endsWith("m")) {
            return (long) (Double.parseDouble(value.replace("m", "")) * 60 * 1000);
        } else if (value.endsWith("h")) {
            return (long) (Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
        } else if (value.endsWith("d")) {
            return (long) (Double.parseDouble(value.replace("d", "")) * 24 * 60 * 60 * 1000);
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public String toString() {
        return milliseconds + " ms";
    }
}