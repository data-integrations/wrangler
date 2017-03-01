package co.cask.wrangler;

/**
 * Thrown when there are issues with pre-conditions.
 */
public class PreconditionException extends Exception {
  public PreconditionException(String message) {
    super(message);
  }
}
