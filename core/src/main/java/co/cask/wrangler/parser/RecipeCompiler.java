/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.parser;

import co.cask.wrangler.api.parser.SyntaxError;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Class description here.
 */
public final class RecipeCompiler implements Compiler {
  private boolean hasErrors;
  private Iterator<SyntaxError> errors;

  public RecipeCompiler() {
    this.hasErrors = false;
    this.errors = new ArrayList<SyntaxError>().iterator();
  }

  @Override
  public Iterator<CompiledUnit> compile(Iterator<String> directives) throws CompileException {
    final List<CompiledUnit> tokens = new ArrayList<>();

    while(directives.hasNext()) {
      String directive = directives.next();
      tokens.add(compile(directive));
    }

    return tokens.iterator();
  }

  @Override
  public CompiledUnit compile(String directives) throws CompileException {
    return compile(CharStreams.fromString(directives));
  }

  @Override
  public CompiledUnit compile(Location location) throws CompileException {
    try {
      return compile(CharStreams.fromStream(location.getInputStream()));
    } catch (IOException e) {
      throw new CompileException(e.getMessage());
    }
  }

  @Override
  public CompiledUnit compile(Path path) throws CompileException {
    try {
      return compile(CharStreams.fromPath(path));
    } catch (IOException e) {
      throw new CompileException(e.getMessage());
    }
  }

  @Override
  public boolean hasErrors() {
    return hasErrors;
  }

  @Override
  public Iterator<SyntaxError> getSyntaxErrors() {
    return errors;
  }

  private CompiledUnit compile(CharStream stream) {
    SyntaxErrorListener errorListener = new SyntaxErrorListener();
    DirectivesLexer lexer = new DirectivesLexer(stream);
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);

    DirectivesParser parser = new DirectivesParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);

    ParseTree tree = parser.directives();
    parser.setBuildParseTree(false);
    RecipeVisitor visitor = new RecipeVisitor();
    visitor.visit(tree);

    if(errorListener.hasErrors()) {
      hasErrors = true;
      errors = errorListener.iterator();
    }

    return visitor.getCompiledUnit();
  }
}
