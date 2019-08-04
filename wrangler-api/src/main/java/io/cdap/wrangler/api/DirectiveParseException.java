/*
 * Copyright © 2017-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (th e "License"); you may not
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

import io.cdap.wrangler.api.parser.SyntaxError;

import java.util.Iterator;

/**
 * An exception thrown when there is error in parsing specification.
 */
public class DirectiveParseException extends Exception implements WranglerErrorCodeProvider {
  private Iterator<SyntaxError> errors;
  private String errorCode;

  public DirectiveParseException(String message, Iterator<SyntaxError> errors) {
    super(message);
    this.errors = errors;
    this.errorCode = ErrorCode.DIRECTIVE_PARSE_ERROR.getCode();
  }

  public DirectiveParseException(String message) {
    super(message);
    this.errorCode = ErrorCode.DIRECTIVE_PARSE_ERROR.getCode();
  }

  public DirectiveParseException(String message, String errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public Iterator<SyntaxError> errors() {
    return errors;
  }

  public String getErrorCode() {
    return errorCode;
  }
}

