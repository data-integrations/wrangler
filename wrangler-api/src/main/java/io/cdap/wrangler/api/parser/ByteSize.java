package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

/**
 * Represents a Token for ByteSize, capable of parsing a size string
 * (e.g., "10KB") and converting it to a canonical unit (bytes).
 */
@PublicEvolving
public class ByteSize implements Token {


    private static final long KILOBYTE = 1024L;
    private static final long MEGABYTE = KILOBYTE * 1024L;
    private static final long GIGABYTE = MEGABYTE * 1024L;

    private final long value;

    /**
     * Constructs a ByteSizeToken by parsing the given size string.
     *
     * @param value The size string to parse (e.g., "10KB", "150MB").
     * @throws IllegalArgumentException If the size string is invalid.
     */
    public ByteSize(String value) {
        this.value = parseSize(value);
    }

    /**
     * Parses the given size string and converts it into bytes.
     *
     * @param sizeString The size string to parse.
     * @return The size in bytes.
     * @throws IllegalArgumentException If the size string is invalid.
     */
    private long parseSize(String sizeString) {
        if (sizeString == null || sizeString.trim().isEmpty()) {
            throw new IllegalArgumentException("Size string must not be null or empty.");
        }

        sizeString = sizeString.trim().toUpperCase();
        if (sizeString.endsWith("KB")) {
            return Long.parseLong(sizeString.replace("KB", "")) * KILOBYTE;
        } else if (sizeString.endsWith("MB")) {
            return Long.parseLong(sizeString.replace("MB", "")) * MEGABYTE;
        } else if (sizeString.endsWith("GB")) {
            return Long.parseLong(sizeString.replace("GB", "")) * GIGABYTE;
        } else if (sizeString.endsWith("B")) {
            return Long.parseLong(sizeString.replace("B", ""));
        } else {
            throw new IllegalArgumentException("Unsupported size unit in string: " + sizeString);
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
     * Returns the size in kilobytes.
     *
     * @return The size in kilobytes.
     */
    public long getKiloBytes() {
        return value/KILOBYTE;
    }

    /**
     * Returns the size in megabytes.
     *
     * @return The size in megabytes.
     */
    public long getMegaBytes() {
        return value/MEGABYTE;
    }

    /**
     * Returns the size in megabytes.
     *
     * @return The size in megabytes.
     */
    public long getGigaBytes() {
        return value/GIGABYTE;
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
        object.addProperty("value", value);
        return object;
    }
}