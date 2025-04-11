package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a Token for TimeDuration, capable of parsing a duration string
 * (e.g., "150ms", "2h") and converting it to a canonical unit (nanoseconds).
 */
@PublicEvolving
public class TimeDuration implements Token {

    // Pattern supports common time units: ns, us, ms, s, m, h, d
    // Allows optional whitespace between number and unit
    private static final Pattern DURATION_PATTERN = Pattern.compile(
            "(\\d+)\\s*(ns|us|ms|s|m|h|d)", Pattern.CASE_INSENSITIVE);

    private final long value;

    /**
     * Constructs a TimeDuration token by parsing the given duration string.
     *
     * @param durationString The duration string to parse (e.g., "150ms", "2h", "1d"). Case-insensitive.
     * @throws IllegalArgumentException If the duration string format is invalid or results in overflow.
     */
    public TimeDuration(String durationString) {
        this.value = parseDuration(durationString);
    }

    /**
     * Parses the given duration string and converts it into nanoseconds.
     *
     * @param token The duration string to parse.
     * @return The duration in nanoseconds.
     * @throws IllegalArgumentException If the duration string format is invalid or results in overflow.
     */
    private long parseDuration(String token) {
        if (token.trim().isEmpty()) {
            throw new IllegalArgumentException("Duration string must not be empty.");
        }

        Matcher matcher = DURATION_PATTERN.matcher(token.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(
                    String.format("Invalid time duration format: '%s'. Expected format like '10s', '150ms', '2h'.", token)
            );
        }

        try {
            long value = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2).toLowerCase();
            long calculatedNanos;

            switch (unit) {
                case "ns":
                    calculatedNanos = value;
                    break;
                case "us":
                    calculatedNanos = TimeUnit.MICROSECONDS.toNanos(value);
                    // Check for overflow: if input > 0 and output <= 0 (and not the trivial 0 case)
                    if (value > 0 && calculatedNanos <= 0) throw new ArithmeticException("long overflow");
                    break;
                case "ms":
                    calculatedNanos = TimeUnit.MILLISECONDS.toNanos(value);
                    if (value > 0 && calculatedNanos <= 0) throw new ArithmeticException("long overflow");
                    break;
                case "s":
                    calculatedNanos = TimeUnit.SECONDS.toNanos(value);
                    if (value > 0 && calculatedNanos <= 0) throw new ArithmeticException("long overflow");
                    break;
                case "m":
                    calculatedNanos = TimeUnit.MINUTES.toNanos(value);
                    if (value > 0 && calculatedNanos <= 0) throw new ArithmeticException("long overflow");
                    break;
                case "h":
                    calculatedNanos = TimeUnit.HOURS.toNanos(value);
                    if (value > 0 && calculatedNanos <= 0) throw new ArithmeticException("long overflow");
                    break;
                case "d":
                    calculatedNanos = TimeUnit.DAYS.toNanos(value);
                    if (value > 0 && calculatedNanos <= 0) throw new ArithmeticException("long overflow");
                    break;
                default:
                    // Should not be reachable due to regex, but included for safety
                    throw new IllegalArgumentException("Unsupported time unit: " + unit);
            }
            return calculatedNanos;

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format("Invalid numeric value in duration string: '%s'", token), e);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    String.format("Duration value '%s' resulted in overflow when converting to nanoseconds.", token), e);
        }
    }

    /**
     * Returns the duration value in the canonical unit (nanoseconds).
     * Note: The Token interface returns Object, but this implementation
     * specifically returns a Long representing nanoseconds.
     *
     * @return The duration in nanoseconds as a Long.
     */
    @Override
    public Long value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    /**
     * Returns the duration in nanoseconds.
     *
     * @return The duration in nanoseconds.
     */
    public long getValue() {
        return value;
    }

    /**
     * Returns the duration in microseconds.
     * Note: Conversion from nanoseconds might truncate.
     *
     * @return The duration in microseconds.
     */
    public long getMicroseconds() {
        return TimeUnit.NANOSECONDS.toMicros(value);
    }

    /**
     * Returns the duration in milliseconds.
     * Note: Conversion from nanoseconds might truncate.
     *
     * @return The duration in milliseconds.
     */
    public long getMilliseconds() {
        return TimeUnit.NANOSECONDS.toMillis(value);
    }

    /**
     * Returns the duration in seconds.
     * Note: Conversion from nanoseconds might truncate.
     *
     * @return The duration in seconds.
     */
    public long getSeconds() {
        return TimeUnit.NANOSECONDS.toSeconds(value);
    }

    /**
     * Returns the duration in minutes.
     * Note: Conversion from nanoseconds might truncate.
     *
     * @return The duration in minutes.
     */
    public long getMinutes() {
        return TimeUnit.NANOSECONDS.toMinutes(value);
    }

    /**
     * Returns the duration in hours.
     * Note: Conversion from nanoseconds might truncate.
     *
     * @return The duration in hours.
     */
    public long getHours() {
        return TimeUnit.NANOSECONDS.toHours(value);
    }

    /**
     * Returns the duration in days.
     * Note: Conversion from nanoseconds might truncate.
     *
     * @return The duration in days.
     */
    public long getDays() {
        return TimeUnit.NANOSECONDS.toDays(value);
    }


    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.TIME_DURATION.name());
        object.addProperty("value", value);
        return object;
    }

}