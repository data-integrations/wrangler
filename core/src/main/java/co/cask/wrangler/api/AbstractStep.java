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

package co.cask.wrangler.api;

/**
 * An abstract class for {@link Step} with added debugging capabilities.
 */
public abstract class AbstractStep implements Step<Record, Record, String> {
  private int lineno;
  private String detail;

  public AbstractStep(int lineno, String detail) {
    this.lineno = lineno;
    this.detail = detail;
  }

  @Override
  public String toString() {
    return String.format("[Step %d] - <%s>", lineno, detail);
  }

  @Override
  public void acceptOptimizerGraphBuilder(OptimizerGraphBuilder<Record, Record, String> optimizerGraphBuilder) {
    optimizerGraphBuilder.buildGraph(this);
  }
}

