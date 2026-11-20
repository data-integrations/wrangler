package io.cdap.wrangler.api.parser;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

/**
 * A simple implementation of the Token interface for testing purposes.
 */
public class SimpleToken implements Token {
    private final TokenType type;
    private final String value;

    public SimpleToken(TokenType type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public TokenType type() {
        return type;
    }

    @Override
    public JsonElement toJson() {
        return new JsonPrimitive(value);
    }
}