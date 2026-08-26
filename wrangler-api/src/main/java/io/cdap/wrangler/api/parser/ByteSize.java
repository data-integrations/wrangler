package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Locale;

public class ByteSize implements Token {
    private final double value;
    private final String unit;

    public ByteSize(String input) {
        input = input.trim().toUpperCase(Locale.ENGLISH);
        this.unit = extractUnit(input);
        this.value = extractNumeric(input);
    }

    private String extractUnit(String input) {
        if (input.endsWith("KB"))
            return "KB";
        if (input.endsWith("MB"))
            return "MB";
        if (input.endsWith("GB"))
            return "GB";
        if (input.endsWith("K"))
            return "KB";
        if (input.endsWith("M"))
            return "MB";
        if (input.endsWith("G"))
            return "GB";
        throw new IllegalArgumentException("Unknown byte size unit: " + input);
    }

    private double extractNumeric(String input) {
        return Double.parseDouble(input.replaceAll("[^\\d.]", ""));
    }

    public long getBytes() {
        switch (unit) {
            case "KB":
                return (long) (value * 1024);
            case "MB":
                return (long) (value * 1024 * 1024);
            case "GB":
                return (long) (value * 1024 * 1024 * 1024);
            default:
                throw new IllegalStateException("Unexpected unit: " + unit);
        }
    }

    @Override
    public Object value() {
        return getBytes();
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(getBytes());
    }
}
