package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        super(value);
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String str) {
        str = str.trim().toUpperCase();
        double number = Double.parseDouble(str.replaceAll("[^0-9.]", ""));
        if (str.endsWith("KB")) return (long)(number * 1024);
        if (str.endsWith("MB")) return (long)(number * 1024 * 1024);
        if (str.endsWith("GB")) return (long)(number * 1024 * 1024 * 1024);
        if (str.endsWith("TB")) return (long)(number * 1024L * 1024 * 1024 * 1024);
        return (long) number; // Bytes
    }

    public long getBytes() {
        return bytes;
    }
}
