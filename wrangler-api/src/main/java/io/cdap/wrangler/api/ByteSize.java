package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        super(value);
        this.bytes = parse(value);
    }

    private long parse(String value) {
        value = value.toUpperCase();
        if (value.endsWith("KB")) return (long)(Double.parseDouble(value.replace("KB", "")) * 1024);
        if (value.endsWith("MB")) return (long)(Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
        if (value.endsWith("GB")) return (long)(Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
        return Long.parseLong(value.replace("B", ""));
    }

    public long getBytes() {
        return bytes;
    }
}