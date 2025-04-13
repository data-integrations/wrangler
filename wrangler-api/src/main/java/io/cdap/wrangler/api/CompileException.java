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

import io.cdap.wrangler.api.annotations.Public;
import io.cdap.wrangler.api.parser.SyntaxError;

import java.util.Iterator;

/**
 * Exception thrown when there are errors compiling a {@link Recipe}.
 * Contains detailed information about syntax errors including line numbers,
 * character positions, and the problematic content.
 *
 * <p>The embedded {@link SyntaxError} objects provide:
 * <ul>
 *   <li>Line number where the error occurred</li>
 *   <li>Character position in the line</li>
 *   <li>Raw content of the erroneous line</li>
 *   <li>Formatted description of the error</li>
 * </ul>
 * </p>
 */
@Public
public class CompileException extends Exception {
  /** Iterator over syntax errors if multiple were found. May be null. */
  private Iterator<SyntaxError> errors;

  /**
   * Creates a compile exception with just an error message.
   *
   * @param message Description of the compilation error
   */
  public CompileException(String message) {
    super(message);
  }

  /**
   * Creates a compile exception with an error message and syntax errors.
   *
   * @param message Description of the compilation error
   * @param errors Iterator over the syntax errors found during compilation
   */
  public CompileException(String message, Iterator<SyntaxError> errors) {
    super(message);
    this.errors = errors;
  }

  /**
   * Creates a compile exception with an error message and cause.
   *
   * @param message Description of the compilation error
   * @param e The underlying exception that caused compilation to fail
   */
  public CompileException(String message, Exception e) {
    super(message, e);
  }

  /**
   * Gets an iterator over any syntax errors found during compilation.
   *
   * @return Iterator over syntax errors, or null if no syntax errors were recorded
   */
  public Iterator<SyntaxError> iterator() {
    return errors;
  }
}
