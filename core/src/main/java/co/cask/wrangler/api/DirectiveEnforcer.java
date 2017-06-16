package co.cask.wrangler.api;

/**
 * Class description here.
 */
public interface DirectiveEnforcer {
  boolean isExcluded(String directive);
}
