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

import io.cdap.wrangler.api.annotations.Public;

/**
 * Specifies the structure for Error records that contain the original row data.
 * This class extends ErrorRecordBase to add row-specific error handling.
 */
@Public
public final class ErrorRecord extends ErrorRecordBase {
  /** The original row that encountered an error. */
  private final Row row;

  /**
   * Creates a new error record with the specified parameters.
   *
   * @param row The row that encountered an error
   * @param message The error message
   * @param code The error code
   * @param showInWrangler Whether to show this error in the Wrangler UI
   */
  public ErrorRecord(Row row, String message, int code, boolean showInWrangler) {
    super(message, code, showInWrangler);
    this.row = row;
  }

  /**
   * Creates a new error record that is not shown in the Wrangler UI.
   *
   * @param row The row that encountered an error
   * @param message The error message
   * @param code The error code
   */
  public ErrorRecord(Row row, String message, int code) {
    this(row, message, code, false);
  }

  /**
   * Gets the original row that encountered an error.
   *
   * @return The row that errored
   */
  public Row getRow() {
    return row;
  }
}
