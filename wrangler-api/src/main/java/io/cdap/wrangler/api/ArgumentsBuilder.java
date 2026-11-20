package io.cdap.wrangler.api;

import com.google.gson.JsonElement;
import io.cdap.wrangler.api.parser.Token;
import io.cdap.wrangler.api.parser.TokenType;

import java.util.HashMap;
import java.util.Map;

public class ArgumentsBuilder {
    private final Map<String, Token> arguments = new HashMap<>();

    public ArgumentsBuilder add(String key, String value) {
        arguments.put(key, new Token() {
            @Override
            public Object value() {
                return value;
            }

            @Override
            public TokenType type() {
                return TokenType.TEXT;
            }

            @Override
            public JsonElement toJson() {
                return null;
            }
        });
        return this;
    }

    public Arguments build() {
        return new Arguments() {
            @Override
            public <T extends Token> T value(String name, String defaultValue) {
                return (T) arguments.getOrDefault(name, new Token() {
                    @Override
                    public Object value() {
                        return defaultValue;
                    }

                    @Override
                    public TokenType type() {
                        return TokenType.TEXT;
                    }

                    @Override
                    public JsonElement toJson() {
                        return null;
                    }
                });
            }

            @Override
            public int size() {
                return arguments.size();
            }

            @Override
            public boolean contains(String name) {
                return arguments.containsKey(name);
            }

            @Override
            public TokenType type(String name) {
                return arguments.containsKey(name) ? arguments.get(name).type() : null;
            }

            @Override
            public int line() {
                return 0;
            }

            @Override
            public int column() {
                return 0;
            }

            @Override
            public String source() {
                return "";
            }

            @Override
            public com.google.gson.JsonElement toJson() {
                return null;
            }
        };
    }
}