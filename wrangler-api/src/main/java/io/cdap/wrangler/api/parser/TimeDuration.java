package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long milliseconds;

    /**
     * Constructs a TimeDuration token by parsing the input token string.
     * Accepts values such as "500ms", "2.1s", "5m", "1h", etc.
     *
     * @param token the token string to parse.
     */
    public TimeDuration(String token) {
        // Normalize token to lowercase and trim whitespace.
        token = token.trim().toLowerCase();
        long multiplier = 1;
        if (token.endsWith("ms")) {
            multiplier = 1;
            token = token.substring(0, token.length() - 2).trim();
        } else if (token.endsWith("s")) {
            multiplier = 1000;
            token = token.substring(0, token.length() - 1).trim();
        } else if (token.endsWith("m")) {
            multiplier = 60 * 1000;
            token = token.substring(0, token.length() - 1).trim();
        } else if (token.endsWith("h")) {
            multiplier = 60 * 60 * 1000;
            token = token.substring(0, token.length() - 1).trim();
        }
        this.milliseconds = (long) (Double.parseDouble(token) * multiplier);
    }

    /**
     * Returns the value in milliseconds.
     *
     * @return value in milliseconds.
     */
    public long getMilliseconds() {
        return milliseconds;
    }
    
    @Override
    public String toString() {
        return milliseconds + " ms";
    }
}
