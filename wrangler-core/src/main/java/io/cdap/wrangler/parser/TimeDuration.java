package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private long millis;

    public TimeDuration(String value) {
        super(value);
        this.millis = parseToMillis(value);
    }

    private long parseToMillis(String value) {
        value = value.toLowerCase().trim();
        if (value.endsWith("ms")) return (long)(Double.parseDouble(value.replace("ms", "")));
        if (value.endsWith("s")) return (long)(Double.parseDouble(value.replace("s", "")) * 1000);
        if (value.endsWith("min")) return (long)(Double.parseDouble(value.replace("min", "")) * 60 * 1000);
        if (value.endsWith("h")) return (long)(Double.parseDouble(value.replace("h", "")) * 60 * 60 * 1000);
        return 0;
    }

    public long getMillis() {
        return millis;
    }
}
