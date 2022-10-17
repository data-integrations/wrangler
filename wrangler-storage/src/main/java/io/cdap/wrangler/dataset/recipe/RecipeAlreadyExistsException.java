package io.cdap.wrangler.dataset.recipe;

/**
 * Thrown when a Recipe with a given recipe name already exists.
 */
public class RecipeAlreadyExistsException extends IllegalArgumentException {
  public RecipeAlreadyExistsException(String message) {
    super(message);
  }
}
