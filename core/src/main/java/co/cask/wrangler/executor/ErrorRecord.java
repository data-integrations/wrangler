/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.wrangler.executor;

import co.cask.wrangler.api.Row;
import co.cask.wrangler.api.annotations.Public;

/**
 * Specifies the structure for Error records.
 */
@Public
public final class ErrorRecord {
  // Actual row that is errored.
  private final Row row;

  // Message as to why the row errored.
  private final String message;

  // Code associated with the message.
  private final int code;

  public ErrorRecord(Row row, String message, int code) {
    this.row = row;
    this.message = message;
    this.code = code;
  }

  /**
   * @return original {@link Row} that errored.
   */
  public Row getRow() {
    return row;
  }

  /**
   * @return Message associated with the {@link Row}.
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return Code associated with the error.
   */
  public int getCode() {
    return code;
  }
}
