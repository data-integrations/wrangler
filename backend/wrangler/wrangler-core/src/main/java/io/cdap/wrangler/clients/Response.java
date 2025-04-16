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

import java.util.List;

/**
 * This class defines the standard response from the Schema registry service.
 * It's paramterized on the type of the response object returned from service within the list.
 */
class Response<T> {
  // Status of the backend processing of the request.
  private int status;

  // Message associated with the status.
  private String message;

  // Number of items in the list.
  private int count;

  // Instance of object in the list.
  private List<T> values;

  Response(int status, String message) {
    this.status = status;
    this.message = message;
  }

  /**
   * @return status of the processing on the backend.
   */
  public int getStatus() {
    return status;
  }

  /**
   * @return Readable form of status of processing in the backend.
   */
  public String getMessage() {
    return message;
  }

  /**
   * @return Count of objects within the list.
   */
  public int getCount() {
    return count;
  }

  /**
   * @return List of the objects of type T.
   */
  public List<T> getValues() {
    return values;
  }
}
