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

package co.cask.wrangler.optimizers;

import co.cask.wrangler.api.DeletionStep;
import co.cask.wrangler.api.KeepStep;
import co.cask.wrangler.api.Optimizer;
import co.cask.wrangler.api.OptimizerGraphBuilder;
import co.cask.wrangler.api.Record;
import co.cask.wrangler.api.SimpleStep;
import co.cask.wrangler.api.Step;
import co.cask.wrangler.api.UnboundedInputOutputStep;
import co.cask.wrangler.api.UnboundedInputStep;
import co.cask.wrangler.api.UnboundedOutputStep;
import com.google.common.base.MoreObjects;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This optimizer removes any steps that are unreachable from the final column output.
 */
public class UnreachableOptimizer implements Optimizer<Record, Record, String>,
  OptimizerGraphBuilder<Record, Record, String> {

  private MutableGraph<StepNode> graph = GraphBuilder.directed().build();
  private Deque<StepNode> unboundedNodes = new LinkedList<>();
  private Map<String, StepNode> lastNode = new HashMap<>();
  private Set<StepNode> genericNodes = new HashSet<>();
  private int stepNumber = 0;
  private Set<Step<Record, Record, String>> keptSteps = new HashSet<>();

  private static class StepNode {
    public final Step<Record, Record, String> step;
    public final int stepNumber;

    public StepNode(Step<Record, Record, String> step, int stepNumber) {
      this.step = step;
      this.stepNumber = stepNumber;
    }
  }

  private void getUsefulSteps(StepNode node) {
    keptSteps.add(node.step);
    for (StepNode child : graph.successors(node)) {
      getUsefulSteps(child);
    }
  }

  private void addEdge(StepNode newNode, String column) {
    StepNode lastBoundedNode = lastNode.get(column);
    StepNode lastUnboundedNode = null;

    for (StepNode node : unboundedNodes) {
      // Note, it is possible to remove this cast using generics but it would force casts to be used elsewhere
      if (((UnboundedOutputStep<Record, Record, String>) node.step).isOutput(column)) {
        lastUnboundedNode = node;
        break;
      }
    }

    StepNode oldNode;
    if (lastBoundedNode == null && lastUnboundedNode == null) {
      return;
    } else if (lastBoundedNode != null && lastUnboundedNode != null) {
      oldNode = lastBoundedNode.stepNumber > lastUnboundedNode.stepNumber ? lastBoundedNode : lastUnboundedNode;
    } else {
      oldNode = MoreObjects.firstNonNull(lastBoundedNode, lastUnboundedNode);
    }

    graph.putEdge(newNode, oldNode);
  }
  @Override
  public void buildGraph(Step<Record, Record, String> step) {
    StepNode newNode = new StepNode(step, ++stepNumber);
    graph.addNode(newNode);

    for (StepNode node : lastNode.values()) {
      graph.putEdge(newNode, node);
    }
    for (StepNode unboundedNode : unboundedNodes) {
      graph.putEdge(newNode, unboundedNode);
    }

    genericNodes.add(newNode);
  }

  @Override
  public void buildGraph(SimpleStep<Record, Record, String> simpleStep) {
    Map<String, StepNode> newLastNode = new HashMap<>(lastNode);

    for (Map.Entry<String, Set<String>> mapEntry : simpleStep.getColumnMap().entrySet()) {
      StepNode newNode = new StepNode(simpleStep, ++stepNumber);
      graph.addNode(newNode);

      String outputColumn = mapEntry.getKey();
      Set<String> inputColumns = mapEntry.getValue();

      for (String inputColumn : inputColumns) {
        addEdge(newNode, inputColumn);
      }

      newLastNode.put(outputColumn, newNode);
    }

    lastNode = newLastNode;
  }

  @Override
  public void buildGraph(DeletionStep<Record, Record, String> deletionStep) {
    keptSteps.add(deletionStep);
    lastNode.keySet().removeAll(deletionStep.getDeletedColumns());
  }

  @Override
  public void buildGraph(KeepStep<Record, Record, String> keepStep) {
    keptSteps.add(keepStep);
    lastNode.keySet().retainAll(keepStep.getKeptColumns());
  }

  @Override
  public void buildGraph(UnboundedInputOutputStep<Record, Record, String> unboundedInputOutputStep) {
    Map<String, StepNode> newLastNode = new HashMap<>(lastNode);

    for (String column : lastNode.keySet()) { // use the set of existing columns to optimize
      Set<String> inputColumnSet = unboundedInputOutputStep.getInputColumns(column);
      if (inputColumnSet != null) { // ensure that column is produced by the step

        StepNode newNode = new StepNode(unboundedInputOutputStep, ++stepNumber);
        graph.addNode(newNode);

        for (String inputColumn : inputColumnSet) {
          addEdge(newNode, inputColumn);
        }

        newLastNode.put(column, newNode);
      }
    }

    keptSteps.add(unboundedInputOutputStep);
    lastNode = newLastNode;
  }

  @Override
  public void buildGraph(UnboundedInputStep<Record, Record, String> unboundedInputStep) {
    Map<String, StepNode> newLastNode = new HashMap<>(lastNode);

    for (String unboundedOutputColumn : unboundedInputStep.getBoundedOutputColumns()) {
      StepNode newNode = new StepNode(unboundedInputStep, ++stepNumber);
      newLastNode.put(unboundedOutputColumn, newNode);
      graph.addNode(newNode);
    }

    for (Map.Entry<String, StepNode> lastNodeEntry : lastNode.entrySet()) {
      Set<String> outputColumns = unboundedInputStep.getOutputColumn(lastNodeEntry.getKey());
      if (outputColumns != null) {
        for (String outputColumn : outputColumns) {
          graph.putEdge(newLastNode.get(outputColumn), lastNodeEntry.getValue());
        }
      }
    }

    lastNode = newLastNode;
  }

  @Override
  public void buildGraph(UnboundedOutputStep<Record, Record, String> unboundedOutputStep) {
    Set<String> boundInputColumns = unboundedOutputStep.getBoundedInputColumns();
    StepNode newNode = new StepNode(unboundedOutputStep, ++stepNumber);

    graph.addNode(newNode);

    for (String boundInputColumn : boundInputColumns) {
      addEdge(newNode, boundInputColumn);
    }

    unboundedNodes.addFirst(newNode);
    keptSteps.add(unboundedOutputStep);
  }

  @Override
  public List<Step<Record, Record, String>> optimize(List<Step<Record, Record, String>> steps) {
    if (stepNumber != 0) {
      throw new IllegalStateException("Cannot use UnreachableOptimizer twice");
    }

    // build the graph
    for (Step<Record, Record, String> step : steps) {
      step.acceptOptimizerGraphBuilder(this);
    }

    Set<Step<Record, Record, String>> usedSteps = keptSteps;
    Set<StepNode> keptNodes = ImmutableSet.<StepNode>builder()
      .addAll(unboundedNodes)
      .addAll(genericNodes)
      .addAll(lastNode.values())
      .build();

    for (StepNode node : keptNodes) {
      getUsefulSteps(node);
    }

    // filter out unused steps
    return ImmutableList.copyOf(Iterables.filter(steps, Predicates.in(usedSteps)));
  }
}
