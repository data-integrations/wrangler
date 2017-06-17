package co.cask.wrangler.api;

/**
 * This class defines the interfaces that would provide the ability to
 * check whether the user should be provided access to the directive being used.
 */
public interface DirectiveEnforcer {

  /**
   * Checks if the directive is being excluded from being used.
   *
   * @param directive to be checked for exclusion.
   * @return true if excluded, false otherwise.
   */
  boolean isExcluded(String directive);
}
