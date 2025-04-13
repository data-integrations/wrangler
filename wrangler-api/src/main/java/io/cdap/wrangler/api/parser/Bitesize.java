public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        // parse like "1.5MB"
        this.bytes = parseToBytes(value);
    }

    public long getBytes() {
        return this.bytes;
    }

    private long parseToBytes(String input) {
        // implement logic based on suffix (e.g., KB, MB, GB)
    }
}
