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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * A {@code Step} which accepts a single column as input and produces a single column as output.
 */
public abstract class AbstractDestinationSourceStep extends AbstractSimpleStep {

  private final String outputColumn;
  private final String inputColumn;

  public AbstractDestinationSourceStep(int lineno, String detail, String outputColumn, String inputColumn) {
    super(lineno, detail);
    this.outputColumn = outputColumn;
    this.inputColumn = inputColumn;
  }

  @Override
  public void acceptOptimizerGraphBuilder(OptimizerGraphBuilder<Record, Record, String> optimizerGraphBuilder) {
    optimizerGraphBuilder.buildGraph(this);
  }

  @Override
  public Map<String, Set<String>> getColumnMap() {
    return ImmutableMap.<String, Set<String>>of(outputColumn, ImmutableSet.of(inputColumn));
  }
}
