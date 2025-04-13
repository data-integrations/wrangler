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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.wrangler.api;

import io.cdap.wrangler.api.parser.SyntaxError;

import java.util.Iterator;

/**
 * Exception thrown when parsing a directive specification fails.
 * Contains details about syntax errors encountered during parsing.
 *
 * <p>This exception can contain:
 * <ul>
 *   <li>A descriptive error message</li>
 *   <li>The name of the directive that failed to parse</li>
 *   <li>An iterator of {@link SyntaxError} objects with detailed error information</li>
 *   <li>An underlying cause exception</li>
 * </ul>
 * </p>
 */
public class DirectiveParseException extends Exception {
  /** Iterator over syntax errors if multiple were found. May be null. */
  private Iterator<SyntaxError> errors;

  /**
   * Creates a parse exception with an error message and syntax errors.
   *
   * @param message Description of the parse error
   * @param errors Iterator over syntax errors found during parsing
   */
  public DirectiveParseException(String message, Iterator<SyntaxError> errors) {
    super(message);
    this.errors = errors;
  }

  /**
   * Creates a parse exception with an error message, syntax errors, and cause.
   *
   * @param message Description of the parse error
   * @param errors Iterator over syntax errors found during parsing
   * @param cause The underlying exception that caused parsing to fail
   */
  public DirectiveParseException(String message, Iterator<SyntaxError> errors, Throwable cause) {
    super(message, cause);
    this.errors = errors;
  }

  /**
   * Creates a parse exception with an error message and cause.
   *
   * @param message Description of the parse error
   * @param cause The underlying exception that caused parsing to fail
   */
  public DirectiveParseException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Creates a parse exception with just a cause.
   *
   * @param cause The underlying exception that caused parsing to fail
   */
  public DirectiveParseException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a parse exception with just an error message.
   *
   * @param message Description of the parse error
   */
  public DirectiveParseException(String message) {
    super(message);
  }

  /**
   * Creates a parse exception with directive name and error details.
   *
   * @param directiveName Name of the directive that failed to parse
   * @param errorMessage Specific error details
   */
  public DirectiveParseException(String directiveName, String errorMessage) {
    this(String.format("Error encountered while parsing '%s': %s", directiveName, errorMessage));
  }

  /**
   * Creates a parse exception with directive name, error details, and cause.
   *
   * @param directiveName Name of the directive that failed to parse
   * @param errorMessage Specific error details
   * @param cause The underlying exception that caused parsing to fail
   */
  public DirectiveParseException(String directiveName, String errorMessage, Throwable cause) {
    this(String.format("Error encountered while parsing '%s': %s", directiveName, errorMessage), cause);
  }

  /**
   * Gets an iterator over any syntax errors found during parsing.
   *
   * @return Iterator over syntax errors, or null if no syntax errors were recorded
   */
  public Iterator<SyntaxError> errors() {
    return errors;
  }
}

