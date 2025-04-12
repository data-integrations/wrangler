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

package io.cdap.wrangler.clients;

/**
 * This is exception thrown when there is issue with request or response.
 */
public class RestClientException extends Exception {
  // Status of the response.
  private final int status;

  // Message associated with the code.
  private final String message;

  public RestClientException(final int status, final String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * @return status code.
   */
  public int getStatus() {
    return status;
  }

  /**
   * @return status message.
   */
  public String getMessage() {
    return message;
  }
}
