package co.cask.wrangler.api.parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Class description here.
 */
public final class ParsedTokens {
  private final List<Token> tokens;

  private ParsedTokens(List<Token> tokens) {
    this.tokens = tokens;
  }

  public Token get(int i) {
    return tokens.get(i);
  }

  public int size() {
    return tokens.size();
  }

  @Override
  public String toString() {
    JsonObject output = new JsonObject();
    output.addProperty("class", this.getClass().getSimpleName());
    output.addProperty("count", tokens.size());
    JsonArray array = new JsonArray();
    for (Token token : tokens) {
      JsonObject object = new JsonObject();
      object.addProperty("token", token.type().toString());
      object.addProperty("value", token.get().toString());
      array.add(object);
    }
    output.add("value", array);
    return output.toString();
  }

  public static ParsedTokens.Builder builder() {
    return new ParsedTokens.Builder();
  }

  public static final class Builder {
    private final List<Token> tokens = new ArrayList<>();

    public Builder() {

    }

    public void add(Token token) {
      tokens.add(token);
    }

    public void addAll(List<Token> tokens) {
      this.tokens.addAll(tokens);
    }

    public ParsedTokens build() {
      return new ParsedTokens(this.tokens);
    }

  }
}
