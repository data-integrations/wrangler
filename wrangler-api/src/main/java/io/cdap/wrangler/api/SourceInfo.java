/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.wrangler.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Class description here.
 */
public final class SourceInfo {
  private final int startLine;
  private final int startCol;
  private final int endLine;
  private final int endCol;
  private final String source;

  public SourceInfo(int lineno, int colno, String source) {
    this.startLine = lineno;
    this.startCol = colno;
    this.endLine = lineno;
    this.endCol = colno;
    this.source = source;
  }
  
  public SourceInfo(int startLine, int startCol, int endLine, int endCol, String source) {
    this.startLine = startLine;
    this.startCol = startCol;
    this.endLine = endLine;
    this.endCol = endCol;
    this.source = source;
  }

  public int getLineNumber() {
    return startLine;
  }

  public int getColumnNumber() {
    return startCol;
  }
  
  public int getEndLineNumber() {
    return endLine;
  }
  
  public int getEndColumnNumber() {
    return endCol;
  }

  public String getSource() {
    return source;
  }

  @Override
  public String toString() {
    return String.format("%3d:%-3d - '%s'", startLine, startCol, source);
  }

  public JsonElement toJson() {
    JsonObject object = new JsonObject();
    object.addProperty("line", startLine);
    object.addProperty("column", startCol);
    object.addProperty("endLine", endLine);
    object.addProperty("endColumn", endCol);
    object.addProperty("source", source);
    return object;
  }
}
