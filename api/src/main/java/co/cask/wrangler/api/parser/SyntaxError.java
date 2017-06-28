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

package co.cask.wrangler.api.parser;

import co.cask.wrangler.api.annotations.PublicEvolving;

/**
 * Class description here.
 */
@PublicEvolving
public final class SyntaxError {
  private final int lineNo;
  private final int charPos;
  private final String message;
  private final String line;

  public SyntaxError(int lineNo, int charPos, String message, String line) {
    this.lineNo = lineNo;
    this.charPos = charPos;
    this.message = message;
    this.line = line;
  }

  public int getLineNumber() {
    return lineNo;
  }

  public int getCharPosition() {
    return charPos;
  }

  public String getMessage() {
    return message;
  }

  public String getLine() {
    return line;
  }
}
