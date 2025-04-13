package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.annotations.PublicEvolving;

@PublicEvolving
public class TimeDuration extends Token {
    private final long nanos;

    public TimeDuration(String value) {
        try {
            // Normalize input (e.g., "5 MS" → "5MS")
            String cleaned = value.replaceAll("\\s+", "").toLowerCase();
           
            // Split into numeric and unit parts (e.g., "1.5m" → "1.5" and "m")
            String numStr = cleaned.replaceAll("[^0-9.]", "");
            String unit = cleaned.replaceAll("[0-9.]", "");

            if (numStr.isEmpty() || unit.isEmpty()) {
                throw new IllegalArgumentException("Invalid time format: " + value);
            }

            double number = Double.parseDouble(numStr);
           
            // Convert to canonical unit (nanoseconds)
            switch (unit) {
                case "ns":
                    nanos = (long) number;
                    break;
                case "ms":
                    nanos = (long) (number * 1_000_000L);
                    break;
                case "s":
                    nanos = (long) (number * 1_000_000_000L);
                    break;
                case "m":  // minutes
                    nanos = (long) (number * 60L * 1_000_000_000L);
                    break;
                case "h":  // hours
                    nanos = (long) (number * 60L * 60L * 1_000_000_000L);
                    break;
                case "d":  // days
                    nanos = (long) (number * 24L * 60L * 60L * 1_000_000_000L);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported time unit: " + unit);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid number in time duration: " + value, e);
        }
    }

    public long getNanos() {
        return nanos;
    }

    // Optional helper method for milliseconds conversion
    public long getMillis() {
        return nanos / 1_000_000;
    }
}
