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
 * This class <code>CompileStatus</code> contains the status of compilation.
 * If there are errors - syntax or semnatic it records the details of the errors
 * as iterator over <code>SyntaxError</code>. If the compilation is successfull,
 * it contains the <code>ExecutableObject</code>.
 */
public final class CompileStatus {
  private RecipeSymbol symbols = null;
  private boolean hasError = false;
  private Iterator<SyntaxError> errors = null;

  public CompileStatus(RecipeSymbol symbols) {
    this.symbols = symbols;
  }

  public CompileStatus(boolean hasError, Iterator<SyntaxError> errors) {
    this.hasError = hasError;
    this.errors = errors;
  }

  public boolean isSuccess() {
    return !hasError;
  }

  public Iterator<SyntaxError> getErrors() {
    if (!hasError) {
      return Collections.emptyIterator();
    }
    return errors;
  }

  public RecipeSymbol getSymbols() {
    return symbols;
  }
}
