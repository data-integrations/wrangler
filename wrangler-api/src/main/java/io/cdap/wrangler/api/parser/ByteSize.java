package io.cdap.wrangler.api.parser;

import com.google.gson.JsonObject;
import io.cdap.wrangler.api.annotations.PublicEvolving;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@PublicEvolving
public class ByteSize implements Token {
    private static final Pattern PATTERN = Pattern.compile("^(\\d+\\.?\\d*)([A-Za-z]+)$");
    private final long bytes;
    private final String original;

    public ByteSize(String value) {
        this.original = value;
        Matcher matcher = PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2).toUpperCase();

        switch (unit) {
            case "B": bytes = (long) number; break;
            case "KB": bytes = (long) (number * 1000); break;
            case "MB": bytes = (long) (number * 1000 * 1000); break;
            case "GB": bytes = (long) (number * 1000 * 1000 * 1000); break;
            case "TB": bytes = (long) (number * 1000L * 1000 * 1000 * 1000); break;
            case "PB": bytes = (long) (number * 1000L * 1000 * 1000 * 1000 * 1000); break;
            case "KIB": bytes = (long) (number * 1024); break;
            case "MIB": bytes = (long) (number * 1024 * 1024); break;
            case "GIB": bytes = (long) (number * 1024 * 1024 * 1024); break;
            case "TIB": bytes = (long) (number * 1024L * 1024 * 1024 * 1024); break;
            case "PIB": bytes = (long) (number * 1024L * 1024 * 1024 * 1024 * 1024); break;
            default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
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
        JsonObject object = new JsonObject();
        object.addProperty("type", type().name());
        object.addProperty("value", original);
        object.addProperty("bytes", bytes);
        return object;
    }

    public long getBytes() {
        return bytes;
    }
    
    public double getBytesAs(String unit) {
        unit = unit.toUpperCase();
        switch (unit) {
            case "B": return bytes;
            case "KB": return bytes / 1000.0;
            case "MB": return bytes / (1000.0 * 1000);
            case "GB": return bytes / (1000.0 * 1000 * 1000);
            case "TB": return bytes / (1000.0 * 1000 * 1000 * 1000);
            case "PB": return bytes / (1000.0 * 1000 * 1000 * 1000 * 1000);
            case "KIB": return bytes / 1024.0;
            case "MIB": return bytes / (1024.0 * 1024);
            case "GIB": return bytes / (1024.0 * 1024 * 1024);
            case "TIB": return bytes / (1024.0 * 1024 * 1024 * 1024);
            case "PIB": return bytes / (1024.0 * 1024 * 1024 * 1024 * 1024);
            default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
    }
}