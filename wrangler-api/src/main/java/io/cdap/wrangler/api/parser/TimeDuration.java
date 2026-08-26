package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class TimeDuration implements Token {
    private final double timeInMillis;

    public TimeDuration(String input) {
        this.timeInMillis = parse(input);
    }

    private double parse(String input) {
        String num = input.replaceAll("[^0-9.]", "");
        String unit = input.replaceAll("[0-9.]", "").toLowerCase();
        double number = Double.parseDouble(num);
        switch (unit) {
            case "ms":
                return number;
            case "s":
                return number * 1000;
            case "m":
                return number * 60 * 1000;
            case "h":
                return number * 60 * 60 * 1000;
            case "d":
                return number * 24 * 60 * 60 * 1000;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
        }
    }

    public double getMilliseconds() {
        return timeInMillis;
    }

    @Override
    public Object value() {
        return timeInMillis;
    }

    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(timeInMillis);
    }
}
