package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ByteSize implements Token {
    private long bytes;

    public ByteSize(String token) {
        // Parse the token string (e.g., "10KB")
        this.bytes = parseByteSize(token);
    }

    private long parseByteSize(String token) {
        // Implement parsing logic here
        String unit = token.replaceAll("[0-9]", "").toUpperCase();
        long value = Long.parseLong(token.replaceAll("[^0-9]", ""));

        switch (unit) {
            case "KB":
                return value * 1024;
            case "MB":
                return value * 1024 * 1024;
            case "GB":
                return value * 1024 * 1024 * 1024;
            default:
                return value; // Assume bytes if no unit is specified
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
        return TokenType.BYTE_SIZE; // Ensure BYTE_SIZE is defined in TokenType
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("bytes", bytes);
        return json;
    }
}