package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TimeDuration implements Token {
    private final long milliseconds;

    public TimeDuration(String token) {
        this.milliseconds = parseMilliseconds(token);
    }

    private long parseMilliseconds(String token) {
        String unit = token.replaceAll("[0-9]", "").trim();
        long value = Long.parseLong(token.replaceAll("[^0-9]", "").trim());
        
        switch (unit.toLowerCase()) {
            case "ms": return value;
            case "s": return value * 1000;
            case "m": return value * 1000 * 60;
            case "h": return value * 1000 * 60 * 60;
            case "d": return value * 1000 * 60 * 60 * 24;
            case "w": return value * 1000 * 60 * 60 * 24 * 7;
            default: throw new IllegalArgumentException("Invalid TimeDuration unit: " + unit);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public Object value() {
        return milliseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type().toString());
        json.addProperty("value", milliseconds);
        return json;
    }
}
