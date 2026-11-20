package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private long bytes;

    public ByteSize(String value) {
        super(value);
        this.bytes = parseToBytes(value);
    }

    private long parseToBytes(String value) {
        value = value.toUpperCase().trim();
        if (value.endsWith("KB")) return (long)(Double.parseDouble(value.replace("KB", "")) * 1024);
        if (value.endsWith("MB")) return (long)(Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
        if (value.endsWith("GB")) return (long)(Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
        if (value.endsWith("B"))  return (long)(Double.parseDouble(value.replace("B", "")));
        return 0;
    }

    public long getBytes() {
        return bytes;
    }
}
