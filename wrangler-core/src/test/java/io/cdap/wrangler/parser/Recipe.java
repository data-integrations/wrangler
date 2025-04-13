/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package io.cdap.wrangler.parser;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a parsed recipe containing a list of directives.
 */
public class Recipe {

  private final List<Directive> directives;

  public Recipe() {
    this.directives = new ArrayList<>();
  }

  public Recipe(List<Directive> directives) {
    this.directives = directives;
  }

  public List<Directive> getDirectives() {
    return directives;
  }

  public void addDirective(Directive directive) {
    directives.add(directive);
  }
}
