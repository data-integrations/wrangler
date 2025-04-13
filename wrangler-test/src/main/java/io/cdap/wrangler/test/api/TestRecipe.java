package io.cdap.wrangler.test.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class TestRecipe {
  private final List<String> directives;

  public TestRecipe() {
    this.directives = new ArrayList<>();
  }

  public void add(String directive) {
    directives.add(directive);
  }

  public List<String> toList() {
    return directives;
  }

  public String[] toArray() {
    return directives.toArray(new String[0]);
  }
}
