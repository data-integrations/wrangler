package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSize implements Token {
    private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)([KMGTP]?B)", Pattern.CASE_INSENSITIVE);

    private final long bytes;

    public ByteSize(String input) {
        Matcher matcher = PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + input);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2).toUpperCase();

        switch (unit) {
            case "B":
                bytes = (long) value;
                break;
            case "KB":
                bytes = (long) (value * 1024);
                break;
            case "MB":
                bytes = (long) (value * 1024 * 1024);
                break;
            case "GB":
                bytes = (long) (value * 1024 * 1024 * 1024);
                break;
            case "TB":
                bytes = (long) (value * 1024 * 1024 * 1024 * 1024);
                break;
            case "PB":
                bytes = (long) (value * 1024 * 1024 * 1024 * 1024 * 1024);
                break;
            default:
                throw new IllegalArgumentException("Unknown unit: " + unit);
        }
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }
}
