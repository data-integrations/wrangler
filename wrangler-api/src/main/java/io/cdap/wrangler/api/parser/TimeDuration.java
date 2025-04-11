// File: wrangler-api/src/main/java/io/cdap/wrangler/api/parser/TimeDuration.java

package io.cdap.wrangler.api.parser;

/**
 * Token class for representing time durations with units (ms, s, m, h, d)
 */
public class TimeDuration extends Token {
    private final long nanos;
    private final String originalValue;

    public TimeDuration(String value) {
        super(TokenType.TIME_DURATION, value);
        this.originalValue = value;
        this.nanos = parseToNanos(value);
    }

    /**
     * Parses a string representation of time duration into nanoseconds
     * 
     * @param value String representation (e.g., "500ms", "2.5s")
     * @return number of nanoseconds
     * @throws IllegalArgumentException if the format is invalid
     */
    private long parseToNanos(String value) {
        try {
            String number = value.replaceAll("[^0-9.]", "");
            String unit = value.replaceAll("[0-9.]", "").toLowerCase();
            double time = Double.parseDouble(number);

            return switch (unit) {
                case "ms" -> (long) (time * 1_000_000); // milliseconds to nanos
                case "s" -> (long) (time * 1_000_000_000); // seconds to nanos
                case "m" -> (long) (time * 60 * 1_000_000_000L); // minutes to nanos
                case "h" -> (long) (time * 3600 * 1_000_000_000L); // hours to nanos
                case "d" -> (long) (time * 86400 * 1_000_000_000L); // days to nanos
                default -> throw new IllegalArgumentException("Invalid time unit: " + unit);
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid time duration format: " + value, e);
        }
    }

    /**
     * @return the duration in nanoseconds
     */
    public long getNanos() {
        return nanos;
    }

    /**
     * @return the duration in milliseconds
     */
    public double getMillis() {
        return nanos / 1_000_000.0;
    }

    /**
     * @return the duration in seconds
     */
    public double getSeconds() {
        return nanos / 1_000_000_000.0;
    }

    /**
     * @return the duration in minutes
     */
    public double getMinutes() {
        return nanos / (60.0 * 1_000_000_000.0);
    }

    /**
     * @return the duration in hours
     */
    public double getHours() {
        return nanos / (3600.0 * 1_000_000_000.0);
    }

    /**
     * @return the duration in days
     */
    public double getDays() {
        return nanos / (86400.0 * 1_000_000_000.0);
    }

    @Override
    public String toString() {
        return originalValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TimeDuration)) return false;
        if (!super.equals(o)) return false;

        TimeDuration that = (TimeDuration) o;
        return nanos == that.nanos;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (nanos ^ (nanos >>> 32));
        return result;
    }
}