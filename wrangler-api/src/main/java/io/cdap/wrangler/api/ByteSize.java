package io.cdap.wrangler.api;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to represent and parse byte size tokens.
 */
// Assuming Token is a placeholder class, define it here if not already defined elsewhere
public class ByteSize extends Object {
    private long bytes;

    // Regular expression to match byte size formats (e.g., "10KB", "150MB", "2GB")
    private static final Pattern BYTE_SIZE_PATTERN = Pattern.compile("^(\\d+(\\.\\d+)?)(KB|MB|GB|bytes)$", Pattern.CASE_INSENSITIVE);

    /**
     * Constructor that parses the token string and converts it to bytes.
     *
     * @param token the byte size token string (e.g., "10KB", "150MB")
     */
    public ByteSize(String token) {
        parseToken(token);
    }

    /**
     * Parses the token string and converts it to bytes.
     *
     * @param token the byte size token string
     */
    private void parseToken(String token) {
        Matcher matcher = BYTE_SIZE_PATTERN.matcher(token.trim());
        if (matcher.matches()) {
            double value = Double.parseDouble(matcher.group(1));
            String unit = matcher.group(3).toUpperCase();

            switch (unit) {
                case "KB":
                    bytes = (long) (value * 1024);
                    break;
                case "MB":
                    bytes = (long) (value * 1024 * 1024);
                    break;
                case "GB":
                    bytes = (long) (value * 1024 * 1024 * 1024);
                    break;
                case "BYTES":
                    bytes = (long) value;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown byte size unit: " + unit);
            }
        } else {
            throw new IllegalArgumentException("Invalid byte size format: " + token);
        }
    }

    /**
     * Retrieves the value in bytes.
     *
     * @return the value in bytes
     */
    public long getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return "ByteSize{" +
                "bytes=" + bytes +
                '}';
    }
}

// ByteSize byteSize = new ByteSize("10MB");
// System.out.println(byteSize.getBytes()); // Outputs: 10485760