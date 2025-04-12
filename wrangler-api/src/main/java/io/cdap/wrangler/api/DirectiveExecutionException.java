package io.cdap.wrangler.api;

/**
 * Exception thrown during directive execution.
 */
public class DirectiveExecutionException extends Exception {
    /**
     * Creates a new DirectiveExecutionException.
     * 
     * @param message The error message
     */
    public DirectiveExecutionException(String message) {
        super(message);
    }
}
