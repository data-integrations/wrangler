package io.cdap.wrangler.api.parser;

/**
 * A class to parse byte size strings like "10KB", "2MB", "1.5GB", etc.
 */
public class ByteSize {
    private final long bytes;

    public ByteSize(String value) {
        this.bytes = parseByteSize(value.trim());
    }

    private long parseByteSize(String value) {
        value = value.toUpperCase();

        if (value.endsWith("KB")) {
            return (long) (Double.parseDouble(value.replace("KB", "")) * 1024);
        } else if (value.endsWith("MB")) {
            return (long) (Double.parseDouble(value.replace("MB", "")) * 1024 * 1024);
        } else if (value.endsWith("GB")) {
            return (long) (Double.parseDouble(value.replace("GB", "")) * 1024 * 1024 * 1024);
        } else if (value.endsWith("TB")) {
            return (long) (Double.parseDouble(value.replace("TB", "")) * 1024L * 1024 * 1024 * 1024);
        } else if (value.endsWith("B")) {
            return (long) (Double.parseDouble(value.replace("B", "")));
        } else {
            throw new IllegalArgumentException("Invalid byte size format: " + value);
        }
    }

    public long getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return bytes + " bytes";
    }
}
