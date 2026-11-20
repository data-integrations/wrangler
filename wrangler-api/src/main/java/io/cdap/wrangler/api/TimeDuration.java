package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;

import java.util.Locale;

public class TimeDuration implements Token {
    private final long millis;

    public TimeDuration(String value) {
        this.millis = parseTimeDuration(value);
    }

    private long parseTimeDuration(String value) {
        String val = value.toLowerCase(Locale.ROOT).trim();
        if (val.endsWith("ms")) {
            return Long.parseLong(val.replace("ms", "").trim());
        } else if (val.endsWith("s")) {
            return Long.parseLong(val.replace("s", "").trim()) * 1000L;
        } else if (val.endsWith("m")) {
            return Long.parseLong(val.replace("m", "").trim()) * 60L * 1000L;
        } else if (val.endsWith("h")) {
            return Long.parseLong(val.replace("h", "").trim()) * 3600L * 1000L;
        } else if (val.endsWith("d")) {
            return Long.parseLong(val.replace("d", "").trim()) * 24L * 3600L * 1000L;
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + value);
        }
    }

    public long getMillis() {
        return millis;
    }

    @Override
    public Object value() {
        return null;
    }

    @Override
    public TokenType type() {
        return null;
    }

    @Override
    public JsonElement toJson() {
        return null;
    }
}
