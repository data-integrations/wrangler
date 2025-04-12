package io.cdap.wrangler.api.parser;

/**
 * Exception thrown for syntax errors during parsing.
 */
public class SyntaxError extends Exception {
    /**
     * Constructor for SyntaxError.
     * 
     * @param message The error message
     */
    public SyntaxError(String message) {
        super(message);
    }
}
