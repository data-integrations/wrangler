package io.cdap.wrangler.api.parser;

import io.cdap.wrangler.api.Optional;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the usage of a directive, including its name and arguments.
 */
public class UsageDefinition {
    private final String name;
    private final List<Argument> arguments;
    
    private UsageDefinition(String name, List<Argument> arguments) {
        this.name = name;
        this.arguments = arguments;
    }
    
    /**
     * Gets the name of the directive.
     * 
     * @return The directive name
     */
    public String getName() {
        return name;
    }
    
    /**
     * Gets the arguments for the directive.
     * 
     * @return List of arguments
     */
    public List<Argument> getArguments() {
        return arguments;
    }
    
    /**
     * Creates a new builder for a UsageDefinition.
     * 
     * @param name The directive name
     * @return A new Builder
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }
    
    /**
     * Argument definition for a directive.
     */
    public static class Argument {
        private final String name;
        private final TokenType type;
        private final Optional<Object> defaultValue;
        
        /**
         * Creates a new Argument.
         * 
         * @param name The argument name
         * @param type The argument type
         * @param defaultValue The default value, if any
         */
        public Argument(String name, TokenType type, Optional<Object> defaultValue) {
            this.name = name;
            this.type = type;
            this.defaultValue = defaultValue;
        }
        
        /**
         * Gets the argument name.
         * 
         * @return The name
         */
        public String getName() {
            return name;
        }
        
        /**
         * Gets the argument type.
         * 
         * @return The type
         */
        public TokenType getType() {
            return type;
        }
        
        /**
         * Gets the default value, if any.
         * 
         * @return The default value
         */
        public Optional<Object> getDefaultValue() {
            return defaultValue;
        }
    }
    
    /**
     * Builder for UsageDefinition.
     */
    public static class Builder {
        private final String name;
        private final List<Argument> arguments;
        
        /**
         * Creates a new Builder.
         * 
         * @param name The directive name
         */
        public Builder(String name) {
            this.name = name;
            this.arguments = new ArrayList<>();
        }
        
        /**
         * Defines a required argument.
         * 
         * @param name The argument name
         * @param type The argument type
         * @return This Builder
         */
        public Builder define(String name, TokenType type) {
            arguments.add(new Argument(name, type, Optional.of(null)));
            return this;
        }
        
        /**
         * Defines an optional argument with a default value.
         * 
         * @param name The argument name
         * @param type The argument type
         * @param defaultValue The default value
         * @return This Builder
         */
        public Builder define(String name, TokenType type, Optional<Object> defaultValue) {
            arguments.add(new Argument(name, type, defaultValue));
            return this;
        }
        
        /**
         * Builds the UsageDefinition.
         * 
         * @return A new UsageDefinition
         */
        public UsageDefinition build() {
            return new UsageDefinition(name, arguments);
        }
    }
}
