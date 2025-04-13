package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;

public class TimeDuration implements Token {
    private final long milliseconds;

    public TimeDuration(String token) {
        super();
        this.milliseconds = parseMilliseconds(token);
    }

    private long parseMilliseconds(String token) {
        token = token.trim().toLowerCase();
        if (token.endsWith("ms")) {
            return Long.parseLong(token.replace("ms", ""));
        } else if (token.endsWith("s")) {
            return Long.parseLong(token.replace("s", "")) * 1000;
        } else if (token.endsWith("min")) {
            return Long.parseLong(token.replace("min", "")) * 60 * 1000;
        } else {
            throw new IllegalArgumentException("Invalid time duration format: " + token);
        }
    }

    public long getMilliseconds() {
        return milliseconds;
    }

    @Override
    public Object value() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'value'");
    }

    @Override
    public TokenType type() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'type'");
    }

    @Override
    public JsonElement toJson() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'toJson'");
    }
}