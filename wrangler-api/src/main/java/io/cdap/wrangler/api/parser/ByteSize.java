package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class ByteSize implements Token {

    private final long bytes;

    public ByteSize(String tokenValue) {
        this.bytes = parseByteSize(tokenValue);
    }

    static long parseByteSize(String tokenValue) {
        tokenValue = tokenValue.toUpperCase().trim();
        long value = Long.parseLong(tokenValue.replaceAll("[^0-9]", ""));
        String unit = tokenValue.replaceAll("[^A-Za-z]", "");

        switch (unit) {
            case "KB":
                return value * 1024;
            case "MB":
                return value * 1024 * 1024;
            case "GB":
                return value * 1024 * 1024 * 1024;
            case "TB":
                return value * 1024 * 1024 * 1024 * 1024;
            default:
                return value;
        }
    }

    public long getBytes() {
        return this.bytes;
    }

    public double getKilobytes() {
        return this.bytes / 1024.0;
    }

    public double getMegabytes() {
        return this.bytes / (1024.0 * 1024);
    }

    public double getGigabytes() {
        return this.bytes / (1024.0 * 1024 * 1024);
    }

    @Override
    public Object value() {
        return this.bytes;
    }

    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(this.bytes);
    }
}
