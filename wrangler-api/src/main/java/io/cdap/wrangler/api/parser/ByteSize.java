package io.cdap.wrangler.api.parser;

public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        super(value);
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        // ...logic to parse "10KB", "1.5MB", etc., and convert to bytes...
    }

    public long getBytes() {
        return bytes;
    }
}
