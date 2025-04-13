package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.annotations.PublicEvolving;

@PublicEvolving
public class ByteSize extends Token {
    private final long bytes;

    public ByteSize(String value) {
        try {
            // Remove any whitespace and convert to uppercase (e.g., "10 kb" -> "10KB")
            String cleaned = value.replaceAll("\\s+", "").toUpperCase();
           
            // Split into numeric part and unit (e.g., "1.5MB" → "1.5" and "MB")
            String numStr = cleaned.replaceAll("[^0-9.]", "");
            String unit = cleaned.replaceAll("[0-9.]", "");
           
            if (numStr.isEmpty() || unit.isEmpty()) {
                throw new IllegalArgumentException("Invalid byte size format: " + value);
            }

            double number = Double.parseDouble(numStr);
           
            // Convert to canonical unit (bytes)
            switch (unit) {
                case "B":
                    bytes = (long) number;
                    break;
                case "KB":
                case "KIB":  // Kibibytes (1024 bytes)
                    bytes = (long) (number * 1024L);
                    break;
                case "MB":
                case "MIB":  // Mebibytes
                    bytes = (long) (number * 1024L * 1024);
                    break;
                case "GB":
                case "GIB":  // Gibibytes
                    bytes = (long) (number * 1024L * 1024 * 1024);
                    break;
                case "TB":
                case "TIB":  // Tebibytes
                    bytes = (long) (number * 1024L * 1024 * 1024 * 1024);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported byte unit: " + unit);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in byte size: " + value, e);
        }
    }

    public long getBytes() {
        return bytes;
    }
}
