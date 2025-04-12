package io.cdap.wrangler.api;

/**
 * Exception thrown during directive parsing.
 */
public class DirectiveParseException extends Exception {
    /**
     * Creates a new DirectiveParseException.
     * 
     * @param message The error message
     */
    public DirectiveParseException(String message) {
        super(message);
    }
}
