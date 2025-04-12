package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TimeDuration implements Token {
    private long value;

    // Constructor to parse the token string (e.g., "10ms", "5s")
    public TimeDuration(String token) {
        // Here, parse the value from the token (e.g., "10ms" -> 10, "5s" -> 5)
        this.value = parseTimeDuration(token);
    }

    private long parseTimeDuration(String token) {
        // Basic parsing logic for time durations
        long duration = 0;
        String unit = token.replaceAll("[^a-zA-Z]", "").toLowerCase(); // Extract unit (e.g., ms, s)
        String numStr = token.replaceAll("[^0-9]", ""); // Extract number (e.g., 10)

        long num = Long.parseLong(numStr);
        switch (unit) {
            case "ms":
                duration = num; // ms stays as is
                break;
            case "s":
                duration = num * 1000; // Convert seconds to milliseconds
                break;
            case "m":
                duration = num * 1000 * 60; // Convert minutes to milliseconds
                break;
            case "h":
                duration = num * 1000 * 60 * 60; // Convert hours to milliseconds
                break;
            default:
                duration = num; // Default is milliseconds
                break;
        }
        return duration;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("value", value);
        return json;
    }

    public long getMilliseconds() {
        return value;
    }
}
