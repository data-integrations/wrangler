package io.cdap.wrangler.api.parser;

public class TimeDuration implements Token {
    private final long millis;

    public TimeDuration(String input) {
        input = input.trim();
        if (input.endsWith("ms")) millis = Long.parseLong(input.replace("ms", ""));
        else if (input.endsWith("s")) millis = Long.parseLong(input.replace("s", "")) * 1000;
        else if (input.endsWith("m")) millis = Long.parseLong(input.replace("m", "")) * 60 * 1000;
        else if (input.endsWith("h")) millis = Long.parseLong(input.replace("h", "")) * 3600 * 1000;
        else throw new IllegalArgumentException("Invalid time duration format: " + input);
    }

    public long getMillis() {
        return millis;
    }

    @Override
    public String toString() {
        return millis + "ms";
    }
}
