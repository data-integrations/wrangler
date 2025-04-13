package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TimeDuration implements Token {
    private final long milliseconds;
    private final String originalValue;

    public TimeDuration(String value) {
        this.originalValue = value;
        this.milliseconds = parseTimeDuration(value);
    }

    private long parseTimeDuration(String value) {
        value = value.trim().toLowerCase();
        if (value.endsWith("ms")) {
            return Long.parseLong(value.replace("ms", "").trim());
        } else if (value.endsWith("s")) {
            return Long.parseLong(value.replace("s", "").trim()) * 1000;
        } else if (value.endsWith("m")) {
            return Long.parseLong(value.replace("m", "").trim()) * 1000 * 60;
        } else if (value.endsWith("h")) {
            return Long.parseLong(value.replace("h", "").trim()) * 1000 * 60 * 60;
        } else if (value.endsWith("d")) {
            return Long.parseLong(value.replace("d", "").trim()) * 1000L * 60 * 60 * 24;
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public Object value() {
        return originalValue;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type().name());
        json.addProperty("value", originalValue);
        json.addProperty("milliseconds", milliseconds);
        return json;
    }
}