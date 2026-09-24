package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class ByteSize implements Token {
    private long value;

    // Constructor to parse the token string (e.g., "10KB")
    public ByteSize(String token) {
        // Here, parse the value from the token (e.g., "10KB" -> 10, "15MB" -> 15)
        this.value = parseByteSize(token);
    }

    private long parseByteSize(String token) {
        // Basic parsing logic
        long size = 0;
        String unit = token.replaceAll("[^a-zA-Z]", "").toUpperCase(); // Extract unit (e.g., KB, MB)
        String numStr = token.replaceAll("[^0-9]", ""); // Extract number (e.g., 10)

        long num = Long.parseLong(numStr);
        switch (unit) {
            case "KB":
                size = num * 1024;
                break;
            case "MB":
                size = num * 1024 * 1024;
                break;
            case "GB":
                size = num * 1024 * 1024 * 1024;
                break;
            default:
                size = num; // Defaults to bytes if no unit is provided
                break;
        }
        return size;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("value", value);
        return json;
    }

    public long getBytes() {
        return value;
    }
}
