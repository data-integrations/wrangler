package io.cdap.wrangler.api.parser;

/**
 * TimeDuration parser for handling time duration units (ns, ms, s, m, h, d)
 */
public class TimeDuration implements Token {
    private final String unit;
    private final double duration;
    private final String originalStr;

    /**
     * Constructor for creating a TimeDuration object from a string.
     * Formats accepted: "100ns", "500ms", "1.5s", "2 m", "24h", "7d", etc.
     * 
     * @param str The string to parse
     * @throws SyntaxError If the string cannot be parsed as a time duration
     */
    public TimeDuration(String str) throws SyntaxError {
        // Save original string
        this.originalStr = str;
        
        // Remove any whitespace
        str = str.trim();
        
        // Check for valid format
        if (str == null || str.isEmpty()) {
            throw new SyntaxError("TimeDuration string cannot be null or empty");
        }
        
        // Find the unit part of the string (e.g., "ns", "ms", "s", etc.)
        int i = 0;
        while (i < str.length() && (Character.isDigit(str.charAt(i)) || str.charAt(i) == '.')) {
            i++;
        }
        
        if (i == 0) {
            throw new SyntaxError("TimeDuration must start with a number: " + str);
        }
        
        if (i == str.length()) {
            throw new SyntaxError("TimeDuration must include a unit (ns, ms, s, m, h, d): " + str);
        }
        
        // Extract the duration and unit
        String durationStr = str.substring(0, i).trim();
        String unitStr = str.substring(i).trim();
        
        try {
            this.duration = Double.parseDouble(durationStr);
        } catch (NumberFormatException e) {
            throw new SyntaxError("Invalid duration format: " + durationStr);
        }
        
        // Validate and normalize the unit
        this.unit = validateAndNormalizeUnit(unitStr);
    }
    
    /**
     * Validates and normalizes the unit string.
     * 
     * @param unit The unit string to validate
     * @return The normalized unit string
     * @throws SyntaxError If the unit is not valid
     */
    private String validateAndNormalizeUnit(String unit) throws SyntaxError {
        // Normalize to lowercase for comparison
        String normalizedUnit = unit.toLowerCase();
        
        // Check for valid units
        switch (normalizedUnit) {
            case "ns":
            case "µs":
            case "us": // Allow both µs and us for microseconds
            case "ms":
            case "s":
            case "m":
            case "h":
            case "d":
                return normalizedUnit;
            default:
                throw new SyntaxError("Invalid time duration unit: " + unit + 
                                     ". Must be one of: ns, µs/us, ms, s, m, h, d");
        }
    }
    
    /**
     * Converts the time duration to the base unit (nanoseconds).
     * 
     * @return The duration in nanoseconds
     */
    @Override
    public Object value() {
        return convertToNanos(duration, unit);
    }
    
    /**
     * Gets the token type.
     * 
     * @return TokenType.TIME_DURATION
     */
    @Override
    public TokenType type() {
        return TokenType.TIME_DURATION;
    }
    
    /**
     * Gets the unit of the time duration.
     * 
     * @return The unit string
     */
    public String getUnit() {
        return unit;
    }
    
    /**
     * Converts the time duration to the specified unit.
     * 
     * @param targetUnit The target unit (ns, µs/us, ms, s, m, h, d)
     * @return The duration in the target unit
     * @throws SyntaxError If the target unit is not valid
     */
    public double convertTo(String targetUnit) throws SyntaxError {
        // Validate target unit
        String normalizedTargetUnit = validateAndNormalizeUnit(targetUnit);
        
        // First convert to nanoseconds
        double nanos = convertToNanos(duration, unit);
        
        // Then convert to target unit
        return convertFromNanos(nanos, normalizedTargetUnit);
    }
    
    /**
     * Converts a duration in the given unit to nanoseconds.
     * 
     * @param duration The duration value
     * @param unit The unit
     * @return The duration in nanoseconds
     */
    private double convertToNanos(double duration, String unit) {
        switch (unit) {
            case "ns":
                return duration;
            case "µs":
            case "us":
                return duration * 1000;
            case "ms":
                return duration * 1000 * 1000;
            case "s":
                return duration * 1000 * 1000 * 1000;
            case "m":
                return duration * 60 * 1000 * 1000 * 1000;
            case "h":
                return duration * 60 * 60 * 1000 * 1000 * 1000;
            case "d":
                return duration * 24 * 60 * 60 * 1000 * 1000 * 1000;
            default:
                return duration; // Should never reach here due to validation
        }
    }
    
    /**
     * Converts a duration in nanoseconds to the target unit.
     * 
     * @param nanos The duration in nanoseconds
     * @param targetUnit The target unit
     * @return The duration in the target unit
     */
    public double convertFromNanos(double nanos, String targetUnit) {
        switch (targetUnit) {
            case "ns":
                return nanos;
            case "µs":
            case "us":
                return nanos / 1000;
            case "ms":
                return nanos / (1000 * 1000);
            case "s":
                return nanos / (1000 * 1000 * 1000);
            case "m":
                return nanos / (60 * 1000 * 1000 * 1000);
            case "h":
                return nanos / (60 * 60 * 1000 * 1000 * 1000);
            case "d":
                return nanos / (24 * 60 * 60 * 1000 * 1000 * 1000);
            default:
                return nanos; // Should never reach here due to validation
        }
    }
    
    /**
     * Returns the original string representation.
     * 
     * @return The original string
     */
    @Override
    public String toString() {
        return originalStr;
    }
}
