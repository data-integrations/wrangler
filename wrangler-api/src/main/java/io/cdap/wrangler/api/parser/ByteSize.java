package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Locale;

public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String value) {
        this.bytes = parseByteSize(value);
    }

    private long parseByteSize(String value) {
        String val = value.toUpperCase(Locale.ROOT).trim();
        try {
            if (val.endsWith("KB")) {
                return Long.parseLong(val.replace("KB", "").trim()) * 1024L;
            } else if (val.endsWith("MB")) {
                return Long.parseLong(val.replace("MB", "").trim()) * 1024L * 1024L;
            } else if (val.endsWith("GB")) {
                return Long.parseLong(val.replace("GB", "").trim()) * 1024L * 1024L * 1024L;
            } else if (val.endsWith("TB")) {
                return Long.parseLong(val.replace("TB", "").trim()) * 1024L * 1024L * 1024L * 1024L;
            } else if (val.endsWith("PB")) {
                return Long.parseLong(val.replace("PB", "").trim()) * 1024L * 1024L * 1024L * 1024L * 1024L;
            } else if (val.endsWith("B")) {
                return Long.parseLong(val.replace("B", "").trim());
            } else {
                throw new IllegalArgumentException("Invalid byte size format: " + value);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in byte size: " + value, e);
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
