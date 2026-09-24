package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public class ByteSize implements Token {
    private final double bytes;

    public ByteSize(String value) {
        this.bytes = parse(value);
    }

    private double parse(String value) {
        String num = value.replaceAll("[^0-9.]", "");
        String unit = value.replaceAll("[0-9.]", "").toUpperCase();
        double number = Double.parseDouble(num);
        switch (unit) {
            case "B":
                return number;
            case "KB":
                return number * 1024;
            case "MB":
                return number * 1024 * 1024;
            case "GB":
                return number * 1024 * 1024 * 1024;
            case "TB":
                return number * 1024L * 1024 * 1024 * 1024;
            case "PB":
                return number * 1024L * 1024 * 1024 * 1024 * 1024;
            default:
                throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
    }

    public double getBytes() {
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
        return new JsonPrimitive(bytes);
    }
}
