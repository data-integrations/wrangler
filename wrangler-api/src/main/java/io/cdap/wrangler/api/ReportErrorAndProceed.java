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
 * Exception throw when the record needs to emitted to error collector.
 * This class is similar to {@link ErrorRowException}, but, with a difference
 * that it reports error and continues processing.
 */
public class ReportErrorAndProceed extends Exception {
  // Message as to why the record errored.
  private String message;

  // Code associated with the error message.
  private int code;

  public ReportErrorAndProceed(String message, int code) {
    this.message = message;
    this.code = code;
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
}
