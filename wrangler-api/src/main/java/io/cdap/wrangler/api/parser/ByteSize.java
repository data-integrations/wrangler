package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents a byte size token that can handle various byte units.
 */
public class ByteSize implements Token {
    private static final Pattern PATTERN = Pattern.compile("(\\d+(?:\\.\\d+)?)\\s*(\\w+)");

    private final double value;
    private final String unit;
    private final long bytes;

    /**
     * Constructor for ByteSize.
     *
     * @param text The string representation of the byte size (e.g., "10KB")
     * @param line The line number in the source
     * @param col The column number in the source
     * @throws TokenException If the format is invalid
     */
    public ByteSize(String text, int line, int col) throws TokenException {
        super();
        Matcher matcher = PATTERN.matcher(text);
        if (matcher.matches()) {
            try {
                this.value = Double.parseDouble(matcher.group(1));
                this.unit = matcher.group(2).toUpperCase(Locale.ROOT);
                this.bytes = toBytes(this.value, this.unit);
            } catch (NumberFormatException e) {
                throw new TokenException("Invalid number format in byte size: " + text);
            }
        } else {
            throw new TokenException("Invalid byte size format: '" + text + "'. Expected format: <number><unit>, e.g., '10KB'");
        }
    }

    /**
     * Converts the byte size to a specific unit.
     *
     * @param targetUnit The target unit (e.g., "MB", "GB")
     * @return The byte size converted to the target unit as a string
     * @throws TokenException If the target unit is not supported
     */
    // In ByteSize.java, update the conversion method
    public String toString(String targetUnit) {
        // Normalize the target unit case
        String normalizedUnit = targetUnit.toUpperCase();

        // Get the bytes in the current unit
        long bytes = getBytes();

        // Convert to the target unit
        double converted;
        if (normalizedUnit.equals("B") || normalizedUnit.equals("BYTES")) {
            converted = bytes;
        } else if (normalizedUnit.equals("KB") || normalizedUnit.equals("KILOBYTES")) {
            converted = bytes / 1024.0;
        } else if (normalizedUnit.equals("MB") || normalizedUnit.equals("MEGABYTES")) {
            converted = bytes / (1024.0 * 1024.0);
        } else if (normalizedUnit.equals("GB") || normalizedUnit.equals("GIGABYTES")) {
            converted = bytes / (1024.0 * 1024.0 * 1024.0);
        } else if (normalizedUnit.equals("TB") || normalizedUnit.equals("TERABYTES")) {
            converted = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
        } else if (normalizedUnit.equals("PB") || normalizedUnit.equals("PETABYTES")) {
            converted = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
        } else if (normalizedUnit.equals("KIB") || normalizedUnit.equals("KIBIBYTES")) {
            converted = bytes / 1024.0;
        } else if (normalizedUnit.equals("MIB") || normalizedUnit.equals("MEBIBYTES")) {
            converted = bytes / (1024.0 * 1024.0);
        } else if (normalizedUnit.equals("GIB") || normalizedUnit.equals("GIBIBYTES")) {
            converted = bytes / (1024.0 * 1024.0 * 1024.0);
        } else if (normalizedUnit.equals("TIB") || normalizedUnit.equals("TEBIBYTES")) {
            converted = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
        } else if (normalizedUnit.equals("PIB") || normalizedUnit.equals("PEBIBYTES")) {
            converted = bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
        } else {
            throw new IllegalArgumentException("Unsupported byte size unit: '" + targetUnit + "'");
        }

        // Format with 2 decimal places
        return String.format("%.2f%s", converted, targetUnit);
    }

    /**
     * @return The byte size in bytes
     */
    public long getBytes() {
        return bytes;
    }

    /**
     * Return a string representation of this byte size.
     */
    @Override
    public String toString() {
        // Remove .00 if the value is a whole number
        if (value == Math.floor(value)) {
            return String.format("%.0f%s",value, unit);
        } else {
            return String.format("%.2f%s", value, unit);
        }
    }

    /**
     * Convert a value from a source unit to bytes.
     */
    private long toBytes(double value, String unit) throws TokenException {
        switch (unit) {
            case "B":
            case "BYTES":
                return (long) value;
            case "K":
            case "KB":
            case "KILOBYTE":
            case "KILOBYTES":
                return (long) (value * 1024);
            case "M":
            case "MB":
            case "MEGABYTE":
            case "MEGABYTES":
                return (long) (value * 1024 * 1024);
            case "G":
            case "GB":
            case "GIGABYTE":
            case "GIGABYTES":
                return (long) (value * 1024 * 1024 * 1024);
            case "T":
            case "TB":
            case "TERABYTE":
            case "TERABYTES":
                return (long) (value * 1024 * 1024 * 1024 * 1024L);
            case "P":
            case "PB":
            case "PETABYTE":
            case "PETABYTES":
                return (long) (value * 1024 * 1024 * 1024 * 1024L * 1024L);
            case "KI":
            case "KIB":
            case "KIBIBYTE":
            case "KIBIBYTES":
                return (long) (value * 1024);
            case "MI":
            case "MIB":
            case "MEBIBYTE":
            case "MEBIBYTES":
                return (long) (value * 1024 * 1024);
            case "GI":
            case "GIB":
            case "GIBIBYTE":
            case "GIBIBYTES":
                return (long) (value * 1024 * 1024 * 1024);
            case "TI":
            case "TIB":
            case "TEBIBYTE":
            case "TEBIBYTES":
                return (long) (value * 1024 * 1024 * 1024 * 1024L);
            case "PI":
            case "PIB":
            case "PEBIBYTE":
            case "PEBIBYTES":
                return (long) (value * 1024 * 1024 * 1024 * 1024L * 1024L);
            default:
                throw new TokenException("Unsupported byte size unit: '" + unit + "'");
        }
    }

    /**
     * Convert bytes to a specified unit.
     */
    private double convertTo(long bytes, String targetUnit) throws TokenException {
        switch (targetUnit) {
            case "B":
            case "BYTES":
                return bytes;
            case "K":
            case "KB":
            case "KILOBYTE":
            case "KILOBYTES":
                return bytes / 1024.0;
            case "M":
            case "MB":
            case "MEGABYTE":
            case "MEGABYTES":
                return bytes / (1024.0 * 1024.0);
            case "G":
            case "GB":
            case "GIGABYTE":
            case "GIGABYTES":
                return bytes / (1024.0 * 1024.0 * 1024.0);
            case "T":
            case "TB":
            case "TERABYTE":
            case "TERABYTES":
                return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
            case "P":
            case "PB":
            case "PETABYTE":
            case "PETABYTES":
                return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
            case "KI":
            case "KIB":
            case "KIBIBYTE":
            case "KIBIBYTES":
                return bytes / 1024.0;
            case "MI":
            case "MIB":
            case "MEBIBYTE":
            case "MEBIBYTES":
                return bytes / (1024.0 * 1024.0);
            case "GI":
            case "GIB":
            case "GIBIBYTE":
            case "GIBIBYTES":
                return bytes / (1024.0 * 1024.0 * 1024.0);
            case "TI":
            case "TIB":
            case "TEBIBYTE":
            case "TEBIBYTES":
                return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0);
            case "PI":
            case "PIB":
            case "PEBIBYTE":
            case "PEBIBYTES":
                return bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0);
            default:
                throw new TokenException("Unsupported byte size unit: '" + targetUnit + "'");
        }
    }

    @Override
    public Object value() {
        return null;
    }

    @Override
    public TokenType type() {
        return null;
    }

    @Override
    public JsonElement toJson() {
        return null;
    }

    public String getUnit() {
        return unit;
    }
}