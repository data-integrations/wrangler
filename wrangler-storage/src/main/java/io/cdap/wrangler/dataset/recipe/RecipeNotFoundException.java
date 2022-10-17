package io.cdap.wrangler.dataset.recipe;

import io.cdap.wrangler.proto.NotFoundException;

/**
 * Thrown when a Recipe is not found when it is expected to exist.
 */
public class RecipeNotFoundException extends NotFoundException {
  public RecipeNotFoundException(String message) {
    super(message);
  }
}
