public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        super("BYTE_SIZE", value);
        this.bytes = parseBytes(value);
    }

    private long parseBytes(String value) {
        value = value.toUpperCase();
        double number = Double.parseDouble(value.replaceAll("[^0-9.]", ""));
        if (value.endsWith("KB")) return (long) (number * 1024);
        if (value.endsWith("MB")) return (long) (number * 1024 * 1024);
        if (value.endsWith("GB")) return (long) (number * 1024 * 1024 * 1024);
        if (value.endsWith("TB")) return (long) (number * 1024L * 1024 * 1024 * 1024);
        return (long) number;
    }

    public long getBytes() {
        return bytes;
    }
}
