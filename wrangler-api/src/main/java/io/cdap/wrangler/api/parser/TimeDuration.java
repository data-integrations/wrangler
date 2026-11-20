package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeDuration implements Token {
    private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)(ms|s|m|h|d)", Pattern.CASE_INSENSITIVE);

    private final long milliseconds;

    public TimeDuration(String input) {
        Matcher matcher = PATTERN.matcher(input.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid time duration format: " + input);
        }

        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2).toLowerCase();

        switch (unit) {
            case "ms":
                milliseconds = (long) value;
                break;
            case "s":
                milliseconds = (long) (value * 1000);
                break;
            case "m":
                milliseconds = (long) (value * 60 * 1000);
                break;
            case "h":
                milliseconds = (long) (value * 60 * 60 * 1000);
                break;
            case "d":
                milliseconds = (long) (value * 24 * 60 * 60 * 1000);
                break;
            default:
                throw new IllegalArgumentException("Unknown time unit: " + unit);
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
        return new JsonPrimitive(milliseconds);
    }
}
