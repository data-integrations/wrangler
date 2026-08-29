package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ByteSize implements Token {
    private final long bytes;

    public ByteSize(String token) {
        this.bytes = parseBytes(token);
    }

    private long parseBytes(String token) {
        String unit = token.replaceAll("[0-9]", "").trim();
        long value = Long.parseLong(token.replaceAll("[^0-9]", "").trim());
        
        switch (unit.toUpperCase()) {
            case "B": return value;
            case "KB": return value * 1024;
            case "MB": return value * 1024 * 1024;
            case "GB": return value * 1024 * 1024 * 1024;
            case "TB": return value * 1024L * 1024 * 1024 * 1024;
            case "PB": return value * 1024L * 1024 * 1024 * 1024 * 1024;
            case "EB": return value * 1024L * 1024 * 1024 * 1024 * 1024 * 1024;
            default: throw new IllegalArgumentException("Invalid ByteSize unit: " + unit);
        }
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public Object value() {
        return bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("type", type().toString());
        json.addProperty("value", bytes);
        return json;
    }
}
