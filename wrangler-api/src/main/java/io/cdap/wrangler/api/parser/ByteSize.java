package io.cdap.wrangler.api.parser;

/**
 * ByteSize parser for handling byte size units (B, KB, MB, GB, etc.)
 */
public class ByteSize implements Token {
    private final String unit;
    private final double size;
    private final String originalStr;

    /**
     * Constructor for creating a ByteSize object from a string.
     * Formats accepted: "1B", "1KB", "1.5MB", "2 GB", etc.
     * 
     * @param str The string to parse
     * @throws SyntaxError If the string cannot be parsed as a byte size
     */
    public ByteSize(String str) throws SyntaxError {
        // Save original string
        this.originalStr = str;
        
        // Remove any whitespace
        str = str.trim();
        
        // Check for valid format
        if (str == null || str.isEmpty()) {
            throw new SyntaxError("ByteSize string cannot be null or empty");
        }
        
        // Find the unit part of the string (e.g., "B", "KB", "MB", etc.)
        int i = 0;
        while (i < str.length() && (Character.isDigit(str.charAt(i)) || str.charAt(i) == '.')) {
            i++;
        }
        
        if (i == 0) {
            throw new SyntaxError("ByteSize must start with a number: " + str);
        }
        
        if (i == str.length()) {
            throw new SyntaxError("ByteSize must include a unit (B, KB, MB, GB, etc.): " + str);
        }
        
        // Extract the size and unit
        String sizeStr = str.substring(0, i).trim();
        String unitStr = str.substring(i).trim();
        
        try {
            this.size = Double.parseDouble(sizeStr);
        } catch (NumberFormatException e) {
            throw new SyntaxError("Invalid size format: " + sizeStr);
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
        // Normalize to uppercase for comparison
        String normalizedUnit = unit.toUpperCase();
        
        // Check for valid units
        switch (normalizedUnit) {
            case "B":
            case "KB":
            case "MB":
            case "GB":
            case "TB":
            case "PB":
                return normalizedUnit;
            default:
                throw new SyntaxError("Invalid byte size unit: " + unit + 
                                     ". Must be one of: B, KB, MB, GB, TB, PB");
        }
    }
    
    /**
     * Converts the byte size to the base unit (bytes).
     * 
     * @return The size in bytes
     */
    @Override
    public Object value() {
        return convertToBytes(size, unit);
    }
    
    /**
     * Gets the token type.
     * 
     * @return TokenType.BYTE_SIZE
     */
    @Override
    public TokenType type() {
        return TokenType.BYTE_SIZE;
    }
    
    /**
     * Gets the unit of the byte size.
     * 
     * @return The unit string
     */
    public String getUnit() {
        return unit;
    }
    
    /**
     * Converts the byte size to the specified unit.
     * 
     * @param targetUnit The target unit (B, KB, MB, GB, TB, PB)
     * @return The size in the target unit
     * @throws SyntaxError If the target unit is not valid
     */
    public double convertTo(String targetUnit) throws SyntaxError {
        // Validate target unit
        String normalizedTargetUnit = validateAndNormalizeUnit(targetUnit);
        
        // First convert to bytes
        double bytes = convertToBytes(size, unit);
        
        // Then convert to target unit
        return convertFromBytes(bytes, normalizedTargetUnit);
    }
    
    /**
     * Converts a size in the given unit to bytes.
     * 
     * @param size The size value
     * @param unit The unit
     * @return The size in bytes
     */
    private double convertToBytes(double size, String unit) {
        switch (unit) {
            case "B":
                return size;
            case "KB":
                return size * 1024;
            case "MB":
                return size * 1024 * 1024;
            case "GB":
                return size * 1024 * 1024 * 1024;
            case "TB":
                return size * 1024 * 1024 * 1024 * 1024;
            case "PB":
                return size * 1024 * 1024 * 1024 * 1024 * 1024;
            default:
                return size; // Should never reach here due to validation
        }
    }
    
    /**
     * Converts a size in bytes to the target unit.
     * 
     * @param bytes The size in bytes
     * @param targetUnit The target unit
     * @return The size in the target unit
     */
    public double convertFromBytes(double bytes, String targetUnit) {
        switch (targetUnit) {
            case "B":
                return bytes;
            case "KB":
                return bytes / 1024;
            case "MB":
                return bytes / (1024 * 1024);
            case "GB":
                return bytes / (1024 * 1024 * 1024);
            case "TB":
                return bytes / (1024 * 1024 * 1024 * 1024);
            case "PB":
                return bytes / (1024 * 1024 * 1024 * 1024 * 1024);
            default:
                return bytes; // Should never reach here due to validation
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
