package io.cdap.wrangler.api.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ByteSize extends Token {
    private static final Pattern BYTE_PATTERN = Pattern.compile("(?i)(\\d+(\\.\\d+)?)(B|KB|MB|GB|TB)");
    private final long bytes;

    public ByteSize(String value) {
        super(value);
        Matcher matcher = BYTE_PATTERN.matcher(value.trim());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }

        double number = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(3).toUpperCase();

        switch (unit) {
            case "B": this.bytes = (long) number; break;
            case "KB": this.bytes = (long) (number * 1024); break;
            case "MB": this.bytes = (long) (number * 1024 * 1024); break;
            case "GB": this.bytes = (long) (number * 1024 * 1024 * 1024); break;
            case "TB": this.bytes = (long) (number * 1024L * 1024 * 1024 * 1024); break;
            default: throw new IllegalArgumentException("Unknown byte unit: " + unit);
        }
    }

    public long getBytes() {
        return bytes;
    }
}
