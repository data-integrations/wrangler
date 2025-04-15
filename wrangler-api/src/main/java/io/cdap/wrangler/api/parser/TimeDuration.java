package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeDuration implements Token {
    private final double value;
    private final String unit;
    private final long nanoseconds;

    // Constructor that directly takes value and unit
    public TimeDuration(String text, int line, int column, double value, String unit) throws TokenException {
        super();
        this.value = value;
        this.unit = unit;

        // Convert to nanoseconds based on unit
        if (unit.equalsIgnoreCase("ns")) {
            this.nanoseconds = (long) value;
        } else if (unit.equalsIgnoreCase("us")) {
            this.nanoseconds = (long) (value * 1000);
        } else if (unit.equalsIgnoreCase("ms")) {
            this.nanoseconds = (long) (value * 1000000);
        } else if (unit.equalsIgnoreCase("s")) {
            this.nanoseconds = (long) (value * 1000000000);
        } else if (unit.equalsIgnoreCase("m") || unit.equalsIgnoreCase("min")) {
            this.nanoseconds = (long) (value * 60 * 1000000000);
        } else if (unit.equalsIgnoreCase("h") || unit.equalsIgnoreCase("hr")) {
            this.nanoseconds = (long) (value * 60 * 60 * 1000000000);
        } else if (unit.equalsIgnoreCase("d")) {
            this.nanoseconds = (long) (value * 24 * 60 * 60 * 1000000000);
        } else {
            throw new TokenException(String.format("Invalid time unit: '%s'", unit));
        }
    }

    // Constructor that parses text
    public TimeDuration(String text, int line, int column) throws TokenException {
        super();

        try {
            // Extract numeric value and unit using regex
            Pattern pattern = Pattern.compile("(\\d+(?:\\.\\d+)?)(\\w+)");
            Matcher matcher = pattern.matcher(text);

            if (matcher.matches()) {
                // Use extracted value and unit but don't redeclare them
                double extractedValue = Double.parseDouble(matcher.group(1));
                String extractedUnit = matcher.group(2).toLowerCase();

                this.value = extractedValue;
                this.unit = extractedUnit;

                // Convert to nanoseconds
                if (extractedUnit.equals("ns")) {
                    this.nanoseconds = (long) extractedValue;
                } else if (extractedUnit.equals("us")) {
                    this.nanoseconds = (long) (extractedValue * 1000);
                }  else if (extractedUnit.equals("ms")) {
                    this.nanoseconds = (long) (extractedValue * 1000000);
                }else if (extractedUnit.equals("s")) {
                    this.nanoseconds = (long) (extractedValue * 1000000000);
                } else if (extractedUnit.equals("m") || extractedUnit.equals("min")) {
                    this.nanoseconds = (long) (extractedValue * 60 * 1000000000);
                } else if (extractedUnit.equals("h") || extractedUnit.equals("hr")) {
                    this.nanoseconds = (long) (extractedValue * 60 * 60 * 1000000000);
                } else if (extractedUnit.equals("d")) {
                    this.nanoseconds = (long) (extractedValue * 24 * 60 * 60 * 1000000000);
                } else {
                    throw new TokenException(String.format("Invalid time unit: '%s'", extractedUnit));
                }
            } else {
                throw new TokenException(String.format(
                        "Invalid time duration format: '%s'. Expected format: <number><unit>, e.g., '150ms'", text));
            }
        } catch (NumberFormatException e) {
            throw new TokenException(String.format(
                    "Invalid time duration value: '%s'", text));
        }
    }

    public long getNanoseconds() {
        return nanoseconds;
    }

    public double getValue() {
        return value;
    }

    public String getUnit() {
        return unit;
    }

    @Override
    public String toString() {
        return String.format("%.2f%s", value, unit);
    }

    public String toString(String targetUnit) {
        double converted;

        // Convert from nanoseconds to the target unit
        if (targetUnit.equalsIgnoreCase("ns")) {
            converted = nanoseconds;
        } else if (targetUnit.equalsIgnoreCase("us")) {
            converted = nanoseconds / 1000.0;
        } else if (targetUnit.equalsIgnoreCase("ms")) {
            converted = nanoseconds / 1000000.0;
        } else if (targetUnit.equalsIgnoreCase("s")) {
            converted = nanoseconds / 1000000000.0;
        } else if (targetUnit.equalsIgnoreCase("m") || targetUnit.equalsIgnoreCase("min")) {
            converted = nanoseconds / (60.0 * 1000000000.0);
        } else if (targetUnit.equalsIgnoreCase("h") || targetUnit.equalsIgnoreCase("hr")) {
            converted = nanoseconds / (60.0 * 60.0 * 1000000000.0);
        } else if (targetUnit.equalsIgnoreCase("d")) {
            converted = nanoseconds / (24.0 * 60.0 * 60.0 * 1000000000.0);
        } else {
            throw new IllegalArgumentException(String.format("Invalid time unit: '%s'", targetUnit));
        }

        return String.format("%.2f%s", converted, targetUnit);
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
}