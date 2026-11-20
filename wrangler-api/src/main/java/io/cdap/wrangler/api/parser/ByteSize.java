// File: wrangler-api/src/main/java/io/cdap/wrangler/api/parser/ByteSize.java

package io.cdap.wrangler.api.parser;

/**
 * Token class for representing byte sizes with units (B, KB, MB, GB, TB)
 */
public class ByteSize extends Token {
    private final long bytes;
    private final String originalValue;

    public ByteSize(String value) {
        super(TokenType.BYTE_SIZE, value);
        this.originalValue = value;
        this.bytes = parseBytes(value);
    }

    /**
     * Parses a string representation of bytes into its numeric value
     * 
     * @param value String representation (e.g., "100KB", "2.5MB")
     * @return number of bytes
     * @throws IllegalArgumentException if the format is invalid
     */
    private long parseBytes(String value) {
        try {
            String number = value.replaceAll("[^0-9.]", "");
            String unit = value.replaceAll("[0-9.]", "").toUpperCase();
            double size = Double.parseDouble(number);

            return switch (unit) {
                case "KB" -> (long) (size * 1024);
                case "MB" -> (long) (size * 1024 * 1024);
                case "GB" -> (long) (size * 1024 * 1024 * 1024);
                case "TB" -> (long) (size * 1024L * 1024L * 1024L * 1024L);
                case "B" -> (long) size;
                default -> throw new IllegalArgumentException("Invalid byte unit: " + unit);
            };
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid byte size format: " + value, e);
        }
    }

    /**
     * @return the size in bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * @return the size in kilobytes
     */
    public double getKilobytes() {
        return bytes / 1024.0;
    }

    /**
     * @return the size in megabytes
     */
    public double getMegabytes() {
        return bytes / (1024.0 * 1024.0);
    }

    /**
     * @return the size in gigabytes
     */
    public double getGigabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0);
    }

    /**
     * @return the size in terabytes
     */
    public double getTerabytes() {
        return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
    }

    @Override
    public String toString() {
        return originalValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteSize)) return false;
        if (!super.equals(o)) return false;

        ByteSize byteSize = (ByteSize) o;
        return bytes == byteSize.bytes;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (int) (bytes ^ (bytes >>> 32));
        return result;
    }
}