package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class TimeDuration implements Token {

    private final long nanoseconds;

    public TimeDuration(String tokenValue) {
        this.nanoseconds = parseTimeDuration(tokenValue);
    }

    static long parseTimeDuration(String tokenValue) {
        tokenValue = tokenValue.toLowerCase().trim();
        long value = Long.parseLong(tokenValue.replaceAll("[^0-9]", ""));
        String unit = tokenValue.replaceAll("[^a-z]", "");

        switch (unit) {
            case "ms":
                return value * 1_000_000;
            case "s":
                return value * 1_000_000_000;
            case "m":
                return value * 60 * 1_000_000_000;
            case "h":
                return value * 3600 * 1_000_000_000;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }

    public long getNanoseconds() {
        return this.nanoseconds;
    }

    public double getMilliseconds() {
        return this.nanoseconds / 1_000_000.0;
    }

    public double getSeconds() {
        return this.nanoseconds / 1_000_000_000.0;
    }

    public double getMinutes() {
        return this.nanoseconds / (60.0 * 1_000_000_000);
    }

    public double getHours() {
        return this.nanoseconds / (3600.0 * 1_000_000_000);
    }

    @Override
    public Object value() {
        return this.nanoseconds;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(this.nanoseconds);
    }
}
