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
 * Exception thrown when there is an error executing a recipe.
 */
public class RecipeException extends Exception {
  /** The row index where the error occurred. */
  private final int rowIndex;

  /** The directive index where the error occurred. */
  private final int directiveIndex;

  /** The message describing the error. */
  private final String message;

  /**
   * Constructs a new recipe exception.
   *
   * @param message The error message
   * @param throwable The underlying cause
   * @param rowIndex The index of the row where error occurred
   * @param directiveIndex The index of the directive where error occurred
   */
  public RecipeException(final String message, final Throwable throwable, 
      final int rowIndex, final int directiveIndex) {
    super(message, throwable);
    this.rowIndex = rowIndex;
    this.directiveIndex = directiveIndex;
    this.message = message;
  }

  /**
   * Constructs a new recipe exception.
   *
   * @param message The error message
   * @param throwable The underlying cause
   * @param directiveIndex The index of the directive where error occurred
   */
  public RecipeException(final String message, final Throwable throwable,
      final int directiveIndex) {
    this(message, throwable, -1, directiveIndex);
  }

  /**
   * Constructs a new recipe exception.
   *
   * @param message The error message
   * @param throwable The underlying cause
   */
  public RecipeException(final String message, final Throwable throwable) {
    this(message, throwable, -1, -1);
  }

  /**
   * Gets the row index where the error occurred.
   *
   * @return The row index, or -1 if not available
   */
  public final int getRowIndex() {
    return rowIndex;
  }

  /**
   * Gets the directive index where the error occurred.
   *
   * @return The directive index, or -1 if not available
   */
  public final int getDirectiveIndex() {
    return directiveIndex;
  }
}
