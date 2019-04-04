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

package io.cdap.wrangler.proto;

/**
 * An exception that has an associated status code.
 */
public class StatusCodeException extends RuntimeException {
  private final int code;

  public StatusCodeException(String s, int code) {
    super(s);
    this.code = code;
  }

  public StatusCodeException(String s, Throwable throwable, int code) {
    super(s, throwable);
    this.code = code;
  }

  public int getCode() {
    return code;
  }
}
