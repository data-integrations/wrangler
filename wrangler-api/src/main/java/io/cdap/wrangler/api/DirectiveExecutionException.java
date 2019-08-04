/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
 * A Executor specific exception used for communicating issues with execution of pipeline in that step.
 */
public class DirectiveExecutionException extends Exception implements WranglerErrorCodeProvider {
  private String errorCode;

  public DirectiveExecutionException(Exception e) {
    super(e);
  }

  public DirectiveExecutionException(String message) {
    super(message);
    this.errorCode = ErrorCode.DIRECTIVE_EXECUTION_ERROR.getCode();
  }

  public DirectiveExecutionException(String message, String errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public DirectiveExecutionException(String s, Throwable e) {
    super(s, e);
  }

  public DirectiveExecutionException(Throwable e) {
    super(e);
  }

  @Override
  public String getErrorCode() {
    return errorCode;
  }
}

