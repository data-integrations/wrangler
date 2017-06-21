package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;

/**
 * Class description here.
 */
@PublicEvolving
public interface Token {
  Object get();
  TokenType type();
}
