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

/**
 * Error codes
 */
public enum ErrorCode {
  // 11 is the prefix for directive related errors.
  DIRECTIVE_PARSE_ERROR("WRA-111001"),
  DIRECTIVE_EXECUTION_ERROR("WRA-111002"),
  DIRECTIVE_LOAD_ERROR("WRA-111003"),
  DIRECTIVE_NOT_FOUND_ERROR("WRA-111004"),
  ERROR_ROW("WRA-111003");

  // 12 is a prefix for runtime exceptions..

  // range with prefix 40xxxx to 42xxxx is for UDDs...

  private String code;

  /**
   *
   * @param code
   */
  ErrorCode(String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
