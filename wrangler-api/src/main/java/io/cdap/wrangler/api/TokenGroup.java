package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.Token;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class description here.
 */
public final class TokenGroup {
  private final SourceInfo info;
  private final List<Token> tokens;

  public TokenGroup() {
    this.info = null;
    this.tokens = new ArrayList<>();
  }

  public TokenGroup(SourceInfo info) {
    this.info = info;
    this.tokens = new ArrayList<>();
  }

  public void add(Token token) {
    tokens.add(token);
  }

  public int size() {
    return tokens.size();
  }

  public Token get(int i) {
    return tokens.get(i);
  }

  public Iterator<Token> iterator() {
    return tokens.iterator();
  }

  public SourceInfo getSourceInfo() {
    return info;
  }
}