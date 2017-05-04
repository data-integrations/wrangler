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

package co.cask.wrangler.api;

import co.cask.wrangler.api.annotations.Public;

/**
 * Specifies the structure for Error records.
 */
@Public
public final class ErrorRecord {
  // Actual record that is errored.
  private final Record record;

  // Message as to why the record errored.
  private final String message;

  // Code associated with the message.
  private final int code;

  public ErrorRecord(Record record, String message, int code) {
    this.record = record;
    this.message = message;
    this.code = code;
  }

  /**
   * @return original {@link Record} that errored.
   */
  public Record getRecord() {
    return record;
  }

  /**
   * @return Message associated with the {@link Record}.
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
