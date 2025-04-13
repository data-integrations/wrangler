package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        super(value);
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        // Parse the value and convert to bytes
        String unit = value.replaceAll("[0-9.]", "").toUpperCase();
        double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
        switch (unit) {
            case "KB": return (long) (number * 1024);
            case "MB": return (long) (number * 1024 * 1024);
            case "GB": return (long) (number * 1024 * 1024 * 1024);
            case "TB": return (long) (number * 1024L * 1024 * 1024 * 1024);
            default: return (long) number; // Assume bytes
        }
    }

    public long getBytes() {
        return bytes;
    }
}
