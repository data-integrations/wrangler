package co.cask.wrangler.api;

/**
 * This interface {@link DirectiveAlias} provides a way to check if a directive
 * is aliased and if there it is aliased to what it is being aliased.
 */
public interface DirectiveAlias {

  /**
   * Checks if the directive is aliased.
   *
   * @param directive to be checked for aliasing.
   * @return true if the directive has an alias, false otherwise.
   */
  boolean hasAlias(String directive);

  /**
   * Returns the root directive aliasee
   * @param directive
   * @return
   */
  String getAlias(String directive);
}
