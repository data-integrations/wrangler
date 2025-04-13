package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ByteSize implements Token {
    private final long bytes;
    private final String originalValue;

    public ByteSize(String value) {
        this.originalValue = value;
        this.bytes = parseByteSize(value);
    }

    private long parseByteSize(String value) {
        value = value.trim().toUpperCase();
        if (value.endsWith("KB")) {
            return Long.parseLong(value.replace("KB", "").trim()) * 1024;
        } else if (value.endsWith("MB")) {
            return Long.parseLong(value.replace("MB", "").trim()) * 1024 * 1024;
        } else if (value.endsWith("GB")) {
            return Long.parseLong(value.replace("GB", "").trim()) * 1024 * 1024 * 1024;
        } else if (value.endsWith("TB")) {
            return Long.parseLong(value.replace("TB", "").trim()) * 1024L * 1024 * 1024 * 1024;
        } else if (value.endsWith("B")) {
            return Long.parseLong(value.replace("B", "").trim());
        } else {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return originalValue;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type().name());
        json.addProperty("value", originalValue);
        json.addProperty("bytes", bytes);
        return json;
    }
}