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
 * Exception thrown when a record needs to be sent to the error collector.
 * This exception carries information about the error including a message,
 * an error code, and visibility settings for the Wrangler UI.
 */
public class ErrorRowException extends Exception {
  /** Detailed message explaining why the record failed processing. */
  private String message;

  /** Numeric code identifying the type of error that occurred. */
  private int code;

  /** Flag indicating if this error should be displayed in the Wrangler UI. */
  private boolean showInWrangler;

  /**
   * Creates an error record with a message, code and UI visibility setting.
   *
   * @param message Detailed error message
   * @param code Numeric error code
   * @param showInWrangler Whether to show this error in the Wrangler UI
   */
  public ErrorRowException(String message, int code, boolean showInWrangler) {
    this(message, code, showInWrangler, null);
  }

  /**
   * Creates an error record with a message, code, UI visibility setting and cause.
   *
   * @param message Detailed error message
   * @param code Numeric error code
   * @param showInWrangler Whether to show this error in the Wrangler UI
   * @param cause The underlying exception that caused this error
   */
  public ErrorRowException(String message, int code, boolean showInWrangler, Throwable cause) {
    super(message, cause);
    this.message = message;
    this.code = code;
    this.showInWrangler = showInWrangler;
  }

  /**
   * Creates an error record with just a message and code.
   * The error will not be shown in the Wrangler UI by default.
   *
   * @param message Detailed error message
   * @param code Numeric error code
   */
  public ErrorRowException(String message, int code) {
    this(message, code, false);
  }

  /**
   * Creates an error record for a specific directive with formatted message.
   *
   * @param directiveName Name of the directive where the error occurred
   * @param errorMessage Specific error details
   * @param code Numeric error code
   */
  public ErrorRowException(String directiveName, String errorMessage, int code) {
    this(directiveName, errorMessage, code, null);
  }

  /**
   * Creates an error record for a specific directive with formatted message and cause.
   *
   * @param directiveName Name of the directive where the error occurred
   * @param errorMessage Specific error details
   * @param code Numeric error code
   * @param cause The underlying exception that caused this error
   */
  public ErrorRowException(String directiveName, String errorMessage, int code, Throwable cause) {
    this(String.format("%s (ecode: %d, directive: %s)", errorMessage, code, directiveName), code, false, cause);
  }

  /**
   * Gets the detailed error message.
   *
   * @return Message explaining why the record failed processing
   */
  @Override
  public String getMessage() {
    return message;
  }

  /**
   * Gets the numeric error code.
   *
   * @return Code identifying the type of error
   */
  public int getCode() {
    return code;
  }

  /**
   * Checks if this error should be displayed in the Wrangler UI.
   *
   * @return true if the error should be shown in the UI, false otherwise
   */
  public boolean isShownInWrangler() {
    return showInWrangler;
  }
}
