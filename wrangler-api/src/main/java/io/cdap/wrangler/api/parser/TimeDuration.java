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

    public static TimeDuration parse(String value) {
        return new TimeDuration(value);
    }

    private long parseTimeDuration(String value) {
        value = value.trim().toLowerCase();
        if (value.endsWith("ms") || value.endsWith("milliseconds")) {
            return (long) Double.parseDouble(value.replace("ms", "").replace("milliseconds", "").trim());
        } else if (value.endsWith("s") || value.endsWith("seconds")) {
            return (long) (Double.parseDouble(value.replace("s", "").replace("seconds", "").trim()) * 1000);
        } else if (value.endsWith("m") || value.endsWith("minutes")) {
            return (long) (Double.parseDouble(value.replace("m", "").replace("minutes", "").trim()) * 1000 * 60);
        } else if (value.endsWith("h") || value.endsWith("hours")) {
            return (long) (Double.parseDouble(value.replace("h", "").replace("hours", "").trim()) * 1000 * 60 * 60);
        } else if (value.endsWith("d") || value.endsWith("days")) {
            return (long) (Double.parseDouble(value.replace("d", "").replace("days", "").trim()) * 1000L * 60 * 60 * 24);
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