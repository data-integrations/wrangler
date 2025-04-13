package io.cdap.wrangler.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to represent and parse time duration tokens.
 */
public class TimeDuration extends Object {
    private long nanoseconds;

    // Regular expression to match time duration formats (e.g., "150ms", "2s", "1.5minutes")
    private static final Pattern TIME_DURATION_PATTERN = Pattern.compile("^(\\d+(\\.\\d+)?)(ms|s|seconds|minutes)$", Pattern.CASE_INSENSITIVE);

    /**
     * Constructor that parses the token string and converts it to nanoseconds.
     *
     * @param token the time duration token string (e.g., "150ms", "2s")
     */
    public TimeDuration(String token) {
        parseToken(token);
    }

    /**
     * Parses the token string and converts it to nanoseconds.
     *
     * @param token the time duration token string
     */
    private void parseToken(String token) {
        Matcher matcher = TIME_DURATION_PATTERN.matcher(token.trim());
        if (matcher.matches()) {
            double value = Double.parseDouble(matcher.group(1));
            String unit = matcher.group(3).toLowerCase();

            switch (unit) {
                case "ms":
                    nanoseconds = (long) (value * 1_000_000); // Convert milliseconds to nanoseconds
                    break;
                case "s":
                case "seconds":
                    nanoseconds = (long) (value * 1_000_000_000); // Convert seconds to nanoseconds
                    break;
                case "minutes":
                    nanoseconds = (long) (value * 60 * 1_000_000_000); // Convert minutes to nanoseconds
                    break;
                default:
                    throw new IllegalArgumentException("Unknown time duration unit: " + unit);
            }
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + token);
        }
    }

    /**
     * Retrieves the value in nanoseconds.
     *
     * @return the value in nanoseconds
     */
    public long getNanoseconds() {
        return nanoseconds;
    }

    @Override
    public String toString() {
        return "TimeDuration{" +
                "nanoseconds=" + nanoseconds +
                '}';
    }
}

// TimeDuration timeDuration = new TimeDuration("150ms");
// System.out.println(timeDuration.getNanoseconds()); // Outputs: 150000000