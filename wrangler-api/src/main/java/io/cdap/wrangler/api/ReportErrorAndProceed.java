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
 * An exception that indicates a non-fatal error occurred during directive
 * execution and processing should continue.
 */
public class ReportErrorAndProceed extends Exception {
  /** The error message. */
  private final String message;

  /** The error code. */
  private final int code;

  /**
   * Creates a new instance with error details.
   *
   * @param message The error message
   * @param code The error code
   */
  public ReportErrorAndProceed(final String message, final int code) {
    super(message);
    this.message = message;
    this.code = code;
  }

  /**
   * Gets the error message.
   *
   * @return The error message
   */
  @Override
  public final String getMessage() {
    return message;
  }

  /**
   * Gets the error code.
   *
   * @return The error code
   */
  public final int getCode() {
    return code;
  }
}
