package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class Time implements Token {
    private final long nanoseconds;

    public Time (String value) {
        this.nanoseconds = parseTimeDuration(value.trim().toLowerCase());
    }

    private long parseTimeDuration(String value) {
        if (value.endsWith("ns")) {
            return Long.parseLong(value.replace("ns", ""));
        } else if (value.endsWith("ms")) {
            return (long)(Double.parseDouble(value.replace("ms", "")) * 1_000_000);
        } else if (value.endsWith("s")) {
            return (long)(Double.parseDouble(value.replace("s", "")) * 1_000_000_000);
        } else if (value.endsWith("m")) {
            return (long)(Double.parseDouble(value.replace("m", "")) * 60 * 1_000_000_000L);
        } else if (value.endsWith("h")) {
            return (long)(Double.parseDouble(value.replace("h", "")) * 3600 * 1_000_000_000L);
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    @Override
    public Object value() {
        return nanoseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(nanoseconds);
    }
}

