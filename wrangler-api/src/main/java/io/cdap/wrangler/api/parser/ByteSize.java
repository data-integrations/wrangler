package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;

public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String token) {
        super();
        this.bytes = parseBytes(token);
    }

    private long parseBytes(String token) {
        token = token.trim().toUpperCase();
        if (token.endsWith("KB")) {
            return Long.parseLong(token.replace("KB", "")) * 1024;
        } else if (token.endsWith("MB")) {
            return Long.parseLong(token.replace("MB", "")) * 1024 * 1024;
        } else if (token.endsWith("GB")) {
            return Long.parseLong(token.replace("GB", "")) * 1024 * 1024 * 1024;
        } else {
            throw new IllegalArgumentException("Invalid byte size format: " + token);
        }
    }

    public long getBytes() {
        return bytes;
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