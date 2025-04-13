/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.SyntaxError;

import java.util.Collections;
import java.util.Iterator;

/**
 * This class {@code CompileStatus} contains the status of compilation.
 * If there are errors - syntax or semantic, it records the details of the errors
 * as an iterator over {@code SyntaxError}. If the compilation is successful,
 * it contains the {@code RecipeSymbol} with compiled directives.
 */
public final class CompileStatus {
  /** The compiled recipe symbols if compilation was successful. */
  private RecipeSymbol symbols = null;
  
  /** Whether compilation encountered any errors. */
  private boolean hasError = false;
  
  /** Iterator over any syntax errors encountered during compilation. */
  private Iterator<SyntaxError> errors = null;

  /**
   * Creates a successful compilation status with recipe symbols.
   *
   * @param symbols The compiled recipe symbols
   */
  public CompileStatus(RecipeSymbol symbols) {
    this.symbols = symbols;
  }

  /**
   * Creates a compilation status for failed compilation with errors.
   *
   * @param hasError Whether compilation had errors
   * @param errors Iterator over the syntax errors encountered
   */
  public CompileStatus(boolean hasError, Iterator<SyntaxError> errors) {
    this.hasError = hasError;
    this.errors = errors;
  }

  /**
   * Checks if compilation was successful.
   *
   * @return true if compilation succeeded, false if there were errors
   */
  public boolean isSuccess() {
    return !hasError;
  }

  /**
   * Gets any syntax errors from compilation.
   *
   * @return Iterator over syntax errors if compilation failed, empty iterator if successful
   */
  public Iterator<SyntaxError> getErrors() {
    if (!hasError) {
      return Collections.emptyIterator();
    }
    return errors;
  }

  /**
   * Gets the compiled recipe symbols.
   *
   * @return RecipeSymbol containing compiled directives if successful, null if failed
   */
  public RecipeSymbol getSymbols() {
    return symbols;
  }
}
