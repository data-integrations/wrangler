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

import java.util.Collections;
import java.util.List;

/**
 * Exception throw when the record needs to emitted to error collector.
 */
public class ErrorRowException extends Exception {
  // Message as to why the record errored.
  private String message;

  // Code associated with the error message.
  private int code;

  private boolean showInWrangler;

  public ErrorRowException(String message, int code, boolean showInWrangler) {
    this.message = message;
    this.code = code;
    this.showInWrangler = showInWrangler;
  }

  public ErrorRowException(String message, int code) {
    this(message, code, false);
  }

  public ErrorRowException(String directiveName, String errorMessage, int code) {
    this(String.format("Error encountered while executing '%s' : %s", directiveName, errorMessage), code);
  }

  /**
   * @return Message as why the record errored.
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return code related to the message.
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
