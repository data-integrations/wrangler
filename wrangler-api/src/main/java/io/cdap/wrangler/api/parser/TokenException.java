package io.cdap.wrangler.api.parser;

/**
 * Exception thrown when there is an error parsing a token.
 */
public class TokenException extends Exception {
    public TokenException(String message) {
        super(message);
    }

    public TokenException(String message, Throwable cause) {
        super(message, cause);
    }
}