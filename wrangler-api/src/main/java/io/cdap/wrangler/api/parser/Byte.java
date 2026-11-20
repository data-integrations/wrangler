package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private final long bytes;

    /**
     * Constructs a ByteSize token by parsing the input token string.
     * Accepts values such as "10KB", "1.5MB", "1024B" etc.
     *
     * @param token the token string to parse.
     */
    public ByteSize(String token) {
        // Normalize token to uppercase and trim whitespace.
        token = token.trim().toUpperCase();
        long multiplier = 1;
        if (token.endsWith("KB")) {
            multiplier = 1024;
            token = token.substring(0, token.length() - 2).trim();
        } else if (token.endsWith("MB")) {
            multiplier = 1024 * 1024;
            token = token.substring(0, token.length() - 2).trim();
        } else if (token.endsWith("GB")) {
            multiplier = 1024 * 1024 * 1024;
            token = token.substring(0, token.length() - 2).trim();
        } else if (token.endsWith("B")) {
            multiplier = 1;
            token = token.substring(0, token.length() - 1).trim();
        }
        // Support decimals (e.g., "1.5MB") and convert to long.
        this.bytes = (long) (Double.parseDouble(token) * multiplier);
    }

    /**
     * Returns the value in bytes.
     *
     * @return value in bytes.
     */
    public long getBytes() {
        return bytes;
    }
    
    @Override
    public String toString() {
        return bytes + " bytes";
    }
}
