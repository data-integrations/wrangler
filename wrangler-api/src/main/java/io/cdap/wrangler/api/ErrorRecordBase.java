/*
 * Copyright © 2019 Cask Data, Inc.
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

import java.util.List;

/**
 * Base class for error record that includes the critical fields.
 */
public class ErrorRecordBase {

  // Message as to why the row errored.
  protected final String message;
  // Code associated with the message.
  protected final int code;
  protected final boolean showInWrangler;

  public ErrorRecordBase(String message, int code, boolean showInWrangler) {
    this.message = message;
    this.code = code;
    this.showInWrangler = showInWrangler;
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

  /**
   * @return Flag indicating whether this record should prevent further wrangling.
   */
  public boolean isShownInWrangler() {
    return showInWrangler;
  }
}
