package io.cdap.wrangler.api.parser;

/**
 * Enum defining token types for the parser.
 */
public enum TokenType {
    TEXT,
    NUMERIC,
    BOOLEAN,
    COLUMN,
    DIRECTIVE_NAME,
    BYTE_SIZE,  // Added for byte size parsing
    TIME_DURATION,  // Added for time duration parsing
    PROPERTIES
}
