package co.cask.wrangler.api;

/**
 * Class description here.
 */
public interface DirectiveAlias {
  boolean hasAlias(String directive);
  String getAlias(String directive);
}
