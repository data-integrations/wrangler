package io.cdap.wrangler.api.parser;

/**
 * Token interface for parsing elements.
 */
public interface Token {
    /**
     * Gets the value of the token.
     * 
     * @return The value object
     */
    Object value();
    
    /**
     * Gets the type of the token.
     * 
     * @return The token type
     */
    TokenType type();
}
