// File: wrangler-api/src/main/java/io/cdap/wrangler/api/parser/TokenFactory.java

package io.cdap.wrangler.api.parser;

/**
 * Factory class for creating tokens from parsed values
 */
public class TokenFactory {
    
    /**
     * Creates a ByteSize token from a string value
     */
    public static ByteSize createByteSize(String value) {
        return new ByteSize(value);
    }

    /**
     * Creates a TimeDuration token from a string value
     */
    public static TimeDuration createTimeDuration(String value) {
        return new TimeDuration(value);
    }

    /**
     * Determines if a string represents a valid byte size
     */
    public static boolean isByteSize(String value) {
        try {
            new ByteSize(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Determines if a string represents a valid time duration
     */
    public static boolean isTimeDuration(String value) {
        try {
            new TimeDuration(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}