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

package io.cdap.wrangler.api.parser;

/**
 * Represents a syntax error encountered during parsing, including location and
 * context information.
 */
public class SyntaxError {
  /** The line number where the error occurred. */
  private final int lineNo;

  /** The character position in the line where the error occurred. */
  private final int charPos;

  /** The error message. */
  private final String message;

  /** The line of text containing the error. */
  private final String line;

  /**
   * Creates a new syntax error.
   *
   * @param lineNo The line number of the error
   * @param charPos The character position of the error
   * @param message The error message
   * @param line The line containing the error
   */
  public SyntaxError(final int lineNo, final int charPos, 
      final String message, final String line) {
    this.lineNo = lineNo;
    this.charPos = charPos;
    this.message = message;
    this.line = line;
  }

  /**
   * Gets the line number.
   *
   * @return The line number
   */
  public int getLineNumber() {
    return lineNo;
  }

  /**
   * Gets the character position.
   *
   * @return The position in the line
   */
  public int getCharacterPosition() {
    return charPos;
  }

  /**
   * Gets the error message.
   *
   * @return The message
   */
  public String getMessage() {
    return message;
  }

  /**
   * Gets the line containing the error.
   *
   * @return The line text
   */
  public String getLine() {
    return line;
  }

  /**
   * Formats this error into a readable message.
   *
   * @return A formatted error message
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("line ").append(lineNo).append(", pos ").append(charPos);
    sb.append(": ").append(message).append("\n");
    sb.append(line).append("\n");
    for (int i = 0; i < charPos; ++i) {
      sb.append(" ");
    }
    sb.append("^");
    return sb.toString();
  }
}
