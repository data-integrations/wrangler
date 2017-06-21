package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;

/**
 * Class description here.
 */
@PublicEvolving
public enum TokenType {
  DIRECTIVE_NAME,
  COLUMN_NAME,
  TEXT,
  NUMERIC,
  BOOLEAN,
  COLUMN_NAME_LIST,
  TEXT_LIST,
  NUMERIC_LIST,
  BOOLEAN_LIST,
  EXPRESSION,
  UUD_DIRECTIVE
}
