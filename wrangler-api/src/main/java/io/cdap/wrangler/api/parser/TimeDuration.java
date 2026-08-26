package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.Locale;

public class TimeDuration implements Token {
    private final double value;
    private final String unit;

    public TimeDuration(String input) {
        input = input.trim().toLowerCase(Locale.ENGLISH);
        this.unit = extractUnit(input);
        this.value = extractNumeric(input);
    }

    private String extractUnit(String input) {
        if (input.endsWith("ms"))
            return "ms";
        if (input.endsWith("s"))
            return "s";
        if (input.endsWith("sec"))
            return "s";
        if (input.endsWith("seconds"))
            return "s";
        throw new IllegalArgumentException("Unknown time duration unit: " + input);
    }

    private double extractNumeric(String input) {
        return Double.parseDouble(input.replaceAll("[^\\d.]", ""));
    }

    public long getMilliseconds() {
        switch (unit) {
            case "ms":
                return (long) value;
            case "s":
                return (long) (value * 1000);
            default:
                throw new IllegalStateException("Unexpected unit: " + unit);
        }
    }

    public long getNanoseconds() {
        return getMilliseconds() * 1_000_000;
    }

    @Override
    public Object value() {
        return getMilliseconds();
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(getMilliseconds());
    }
}
