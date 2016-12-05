/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.wrangler.api;

/**
 * A interface defining the wrangle step in the wrangling pipeline.
 */
public abstract class AbstractStep implements Step<Row, Row> {
  private int lineno;
  private String detail;

  public AbstractStep(int lineno, String detail) {
    this.lineno = lineno;
    this.detail = detail;
  }

  public int getLineNo() {
    return lineno;
  }

  public String getDetail() {
    return detail;
  }

  @Override
  public String toString() {
    return String.format("[Step %d] - %s", lineno, detail);
  }
}

