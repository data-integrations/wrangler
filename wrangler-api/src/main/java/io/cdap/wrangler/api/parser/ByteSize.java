package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Represents a Token for ByteSize, capable of parsing a size string
 * (e.g., "10KB", "1.5MB") and converting it to a canonical unit (bytes).
 */
@PublicEvolving
public class ByteSize implements Token {

    // Use double for multipliers to handle fractional input accurately before casting
    private static final double KILOBYTE = 1024.0;
    private static final double MEGABYTE = KILOBYTE * 1024.0;
    private static final double GIGABYTE = MEGABYTE * 1024.0;
    private static final double TERABYTE = GIGABYTE * 1024.0; // Added Terabyte

    private final long value; // Store final value in bytes as long

    /**
     * Constructs a ByteSizeToken by parsing the given size string.
     *
     * @param value The size string to parse (e.g., "10KB", "1.5MB").
     * @throws IllegalArgumentException If the size string is invalid.
     */
    public ByteSize(String value) {
        this.value = parseSize(value);
    }

    /**
     * Parses the given size string and converts it into bytes.
     * Handles integer and floating-point numbers.
     *
     * @param sizeString The size string to parse.
     * @return The size in bytes (truncated to long).
     * @throws IllegalArgumentException If the size string format or unit is invalid.
     */
    private long parseSize(String sizeString) {
        if (sizeString == null || sizeString.trim().isEmpty()) {
            throw new IllegalArgumentException("Size string must not be null or empty.");
        }

        sizeString = sizeString.trim().toUpperCase();
        String numericPart;
        double multiplier;

        try {
            if (sizeString.endsWith("KB")) {
                numericPart = sizeString.substring(0, sizeString.length() - 2);
                multiplier = KILOBYTE;
            } else if (sizeString.endsWith("MB")) {
                numericPart = sizeString.substring(0, sizeString.length() - 2);
                multiplier = MEGABYTE;
            } else if (sizeString.endsWith("GB")) {
                numericPart = sizeString.substring(0, sizeString.length() - 2);
                multiplier = GIGABYTE;
            } else if (sizeString.endsWith("TB")) { // Added Terabyte
                numericPart = sizeString.substring(0, sizeString.length() - 2);
                multiplier = TERABYTE;
            } else if (sizeString.endsWith("B")) {
                numericPart = sizeString.substring(0, sizeString.length() - 1);
                multiplier = 1.0;
            } else {
                // Match the test's expected generic message format
                throw new IllegalArgumentException("Invalid byte size format or unsupported unit in string: " + sizeString);
            }

            if (numericPart.isEmpty()) {
                throw new IllegalArgumentException("Missing numeric value in size string: " + sizeString);
            }

            double parsedValue = Double.parseDouble(numericPart);
            if (parsedValue < 0) {
                throw new IllegalArgumentException("Size value cannot be negative: " + sizeString);
            }
            // Cast to long truncates fractional bytes, which is reasonable for byte sizes.
            return (long) (parsedValue * multiplier);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value in size string: " + sizeString, e);
        }
    }

    /**
     * Returns the size in bytes.
     *
     * @return The size in bytes.
     */
    public long getBytes() {
        return value;
    }

    /**
     * Returns the size in kilobytes (double for potential fractions).
     *
     * @return The size in kilobytes.
     */
    public double getKiloBytes() {
        return value / KILOBYTE;
    }

    /**
     * Returns the size in megabytes (double for potential fractions).
     *
     * @return The size in megabytes.
     */
    public double getMegaBytes() {
        return value / MEGABYTE;
    }

    /**
     * Returns the size in gigabytes (double for potential fractions).
     *
     * @return The size in gigabytes.
     */
    public double getGigaBytes() {
        return value / GIGABYTE;
    }

    /**
     * Returns the size in terabytes (double for potential fractions).
     *
     * @return The size in terabytes.
     */
    public double getTeraBytes() {
        return value / TERABYTE;
    }


    @Override
    public Object value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject object = new JsonObject();
        object.addProperty("type", TokenType.BYTE_SIZE.name());
        object.addProperty("value", value); // Store the canonical long value
        return object;
    }

    @Override
    public String toString() {
        // Provide a reasonable string representation, maybe the original input if stored,
        // or reconstruct from the byte value. For simplicity, just return bytes.
        return value + "B";
    }
}