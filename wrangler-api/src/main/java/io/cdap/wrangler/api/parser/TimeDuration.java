package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TimeDuration implements Token {
    private long nanoseconds;

    public TimeDuration(String token) {
        this.nanoseconds = parseTimeDuration(token);
    }

    private long parseTimeDuration(String token) {
        String unit = token.replaceAll("[0-9]", "").toLowerCase();
        long value = Long.parseLong(token.replaceAll("[^0-9]", ""));

        switch (unit) {
            case "ms":
                return value * 1_000_000;
            case "s":
                return value * 1_000_000_000;
            case "m":
                return value * 60 * 1_000_000_000;
            case "h":
                return value * 60 * 60 * 1_000_000_000;
            case "d":
                return value * 24 * 60 * 60 * 1_000_000_000;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
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
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("nanoseconds", nanoseconds);
        return json;
    }
}
