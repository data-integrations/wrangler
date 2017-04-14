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
 * This interface builds optimization graphs by accepting the subtypes of {@code Step} and handling each one.
 */
public interface OptimizerGraphBuilder<I, O, C> {
  void buildGraph(Step<I, O, C> step);
  void buildGraph(SimpleStep<I, O, C> simpleStep);
  void buildGraph(DeletionStep<I, O, C> deletionStep);
  void buildGraph(KeepStep<I, O, C> keepStep);
  void buildGraph(UnboundedInputOutputStep<I, O, C> unboundedInputOutputStep);
  void buildGraph(UnboundedInputStep<I, O, C> unboundedInputStep);
  void buildGraph(UnboundedOutputStep<I, O, C> unboundedOutputStep);
}
