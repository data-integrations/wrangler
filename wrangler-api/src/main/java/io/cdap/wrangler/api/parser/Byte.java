package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class Byte implements Token {
    private final long bytes;

    public Byte(String value) {
        this.bytes = parseByteSize(value.trim().toUpperCase());
    }

    private long parseByteSize(String value) {
        if (value.endsWith("KB")) {
            return (long)(Double.parseDouble(value.replace("KB", "")) * 1024);
        } else if (value.endsWith("MB")) {
            return (long)(Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
        } else if (value.endsWith("GB")) {
            return (long)(Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
        } else if (value.endsWith("B")) {
            return Long.parseLong(value.replace("B", ""));
        } else {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
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
        return TokenType.BYTE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(bytes);
    }
}
