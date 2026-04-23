package io.cdap.wrangler.api.parser;

public class TimeDuration extends Token {
    private final long milliseconds;

    public TimeDuration(String value) {
        super(value);
        this.milliseconds = parse(value);
    }

    private long parse(String value) {
        value = value.toLowerCase();
        if (value.endsWith("ms")) return (long)(Double.parseDouble(value.replace("ms", "")));
        if (value.endsWith("s")) return (long)(Double.parseDouble(value.replace("s", "")) * 1000);
        return 0;
    }

    public long getMilliseconds() {
        return milliseconds;
    }
}