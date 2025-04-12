package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TimeDuration implements Token {
    private long nanoseconds;

    public TimeDuration(String token) {
        // Parse the token string (e.g., "150ms")
        this.nanoseconds = parseTimeDuration(token);
    }

    private long parseTimeDuration(String token) {
        // Implement parsing logic here
        String unit = token.replaceAll("[0-9]", "").toLowerCase();
        long value = Long.parseLong(token.replaceAll("[^0-9]", ""));

        switch (unit) {
            case "ms":
                return value * 1_000_000; // Convert milliseconds to nanoseconds
            case "s":
                return value * 1_000_000_000; // Convert seconds to nanoseconds
            case "minutes":
                return value * 60 * 1_000_000_000; // Convert minutes to nanoseconds
            default:
                return value; // Assume nanoseconds if no unit is specified
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
        return TokenType.TIME_DURATION; // Ensure TIME_DURATION is defined in TokenType
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("nanoseconds", nanoseconds);
        return json;
    }
}