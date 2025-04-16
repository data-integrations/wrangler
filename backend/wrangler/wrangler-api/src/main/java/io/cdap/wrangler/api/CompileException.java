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
 * This class <code>CompileException</code> is thrown when there is
 * any issue with compiling the <code>Recipe</code>. It's content
 * include the exact line where the error occured and guess of what
 * the problem is. Often times, the guess is close enough to point
 * the problem, but, it's fair attempt to detect the exact issue.
 *
 * The <code>SyntaxError</code> object embedded within this exception
 * contains the line number, character position, the raw content and
 * the formatted content of the line.
 */
@Public
public class CompileException extends Exception {
  private Iterator<SyntaxError> errors;

  public CompileException(String message) {
    super(message);
  }

  public CompileException(String message, Iterator<SyntaxError> errors) {
    super(message);
    this.errors = errors;
  }

  public CompileException(String message, Exception e) {
    super(message, e);
  }

  public Iterator<SyntaxError> iterator() {
    return errors;
  }
}
