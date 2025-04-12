package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private long bytes;

    public ByteSize(String value) {
        super(value);
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        String unit = value.replaceAll("[\\d.]", "").toUpperCase();
        double number = Double.parseDouble(value.replaceAll("[^\\d.]", ""));
        switch (unit) {
            case "KB": return (long)(number * 1024);
            case "MB": return (long)(number * 1024 * 1024);
            case "GB": return (long)(number * 1024 * 1024 * 1024);
            case "TB": return (long)(number * 1024L * 1024L * 1024L * 1024L);
            case "B":
            default: return (long) number;
        }
    }

    public long getBytes() {
        return bytes;
    }
}
