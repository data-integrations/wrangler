package io.cdap.wrangler.api;

/**
 * Context for directive execution.
 */
public interface ExecutorContext {
    /**
     * Environment types.
     */
    enum Environment {
        SERVICE,
        TRANSFORM,
        PREVIEW
    }
    
    /**
     * Gets the environment.
     * 
     * @return The environment
     */
    Environment getEnvironment();
}
