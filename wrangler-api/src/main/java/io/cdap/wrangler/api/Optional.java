package io.cdap.wrangler.api;

/**
 * Simple Optional class for handling null values.
 * 
 * @param <T> The type of the value
 */
public class Optional<T> {
    private final T value;
    
    private Optional(T value) {
        this.value = value;
    }
    
    /**
     * Creates an Optional containing the given value.
     * 
     * @param <T> The type of the value
     * @param value The value
     * @return An Optional containing the value
     */
    public static <T> Optional<T> of(T value) {
        return new Optional<>(value);
    }
    
    /**
     * Returns whether the value is present.
     * 
     * @return true if the value is not null, false otherwise
     */
    public boolean isPresent() {
        return value != null;
    }
    
    /**
     * Gets the value.
     * 
     * @return The value
     */
    public T get() {
        return value;
    }
}
