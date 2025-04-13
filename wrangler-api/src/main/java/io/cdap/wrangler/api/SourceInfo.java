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

/**
 * Information about the source location of a directive or token.
 */
public class SourceInfo {
  /** The line number in the source. */
  private final int lineno;

  /** The column number in the source. */
  private final int colno;

  /** The source identifier (e.g. filename). */
  private final String source;

  /**
   * Creates a new source info.
   *
   * @param lineno The line number
   * @param colno The column number 
   * @param source The source identifier
   */
  public SourceInfo(final int lineno, final int colno, final String source) {
    this.lineno = lineno;
    this.colno = colno;
    this.source = source;
  }

  /**
   * Gets the line number.
   *
   * @return The line number
   */
  public int getLine() {
    return lineno;
  }

  /**
   * Gets the column number.
   *
   * @return The column number
   */
  public int getColumn() {
    return colno;
  }

  /**
   * Gets the source identifier.
   *
   * @return The source identifier
   */
  public String getSource() {
    return source;
  }

  @Override
  public String toString() {
    return String.format("%s:%d:%d", source, lineno, colno);
  }
}
