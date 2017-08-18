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

package co.cask.wrangler.lineage;

import co.cask.wrangler.api.lineage.LineageGenerationException;
import co.cask.wrangler.api.lineage.Mutation;
import co.cask.wrangler.api.lineage.MutationDefinition;
import co.cask.wrangler.api.lineage.MutationType;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.unix4j.Unix4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

/**
 * WranglerFieldLevelLineage is a data type for computing lineage for all columns in Wrangler.
 * An instance of this type to be sent to platform through prepareRun API.
 */
public final class WranglerFieldLevelLineage implements FieldLevelLineage {
  /**
   * A WranglerBranchingStepNode is a data type for linking columns to directives as well as other columns.
   */
  public final class WranglerBranchingStepNode implements BranchingTransformStepNode {
    private final int stepNumber;
    private boolean continueUp;
    private boolean continueDown;
    private final Map<String, Integer> upBranches;
    private final Map<String, Integer> downBranches;

    private WranglerBranchingStepNode(int stepNumber, boolean continueUp, boolean continueDown) {
      this.stepNumber = stepNumber;
      this.continueUp = continueUp;
      this.continueDown = continueDown;
      this.upBranches = new HashMap<>();
      this.downBranches = new HashMap<>();
    }

    private void putRead(String columnName) {
      downBranches.put(columnName, lineage.get(columnName).size());
    }

    private void putLineage(String columnName) {
      upBranches.put(columnName, lineage.get(columnName).size() - 2);
    }

    @Override
    public int getTransformStepNumber() {
      return stepNumber;
    }

    @Override
    public boolean continueBackward() {
      return continueUp;
    }

    @Override
    public boolean continueForward() {
      return continueDown;
    }

    @Override
    public Map<String, Integer> getImpactedBranches() {
      return Collections.unmodifiableMap(downBranches);
    }

    @Override
    public Map<String, Integer> getImpactingBranches() {
      return Collections.unmodifiableMap(upBranches);
    }

    @Override
    public String toString() {
      return "(StepNumber: " + stepNumber + ", Continue Backward: " + continueUp + ", " + upBranches +
          ", Continue Forward: " + continueDown + ", " + downBranches + ")";
    }
  }

  private final List<TransformStep> steps;
  private final Map<String, List<BranchingTransformStepNode>> lineage;

  private WranglerFieldLevelLineage(List<String> startColumns) {
    this.steps = new ArrayList<>();
    this.lineage = new HashMap<>();
    for (String column : startColumns) {
      lineage.put(column, new ArrayList<BranchingTransformStepNode>());
    }
  }

  private WranglerBranchingStepNode createBranchingStep(int number) {
    return new WranglerBranchingStepNode(number, true, true);
  }

  private WranglerBranchingStepNode addBranchingStep(String column, int number, boolean up, boolean down) {
    WranglerBranchingStepNode node = new WranglerBranchingStepNode(number, up, down);
    lineage.get(column).add(node);
    return node;
  }

  private WranglerBranchingStepNode getLast(String column) {
    return (WranglerBranchingStepNode) lineage.get(column).get(lineage.get(column).size() - 1);
  }

  private void prettyPrintFutures(String column, int index, String indent, boolean start, boolean last) {
    System.out.print(indent);
    if (last) {
      System.out.print("\\-");
      indent += "  ";
    } else {
      System.out.print("|-");
      indent += "| ";
    }
    if (start) {
      System.out.println("Column: " + column);
      if (index < lineage.get(column).size()) {
        prettyPrintFutures(column, index, indent, false, true);
      }
    } else {
      WranglerBranchingStepNode node = (WranglerBranchingStepNode) lineage.get(column).get(index++);
      System.out.println(steps.get(node.getTransformStepNumber()));
      if (node.continueDown && index < lineage.get(column).size()) {
        prettyPrintFutures(column, index, indent, false, node.downBranches.isEmpty());
      }
      int i = 0;
      for (Map.Entry<String, Integer> entry : node.downBranches.entrySet()) {
        prettyPrintFutures(entry.getKey(), entry.getValue(), indent, true, ++i == node.downBranches.size());
      }
    }
  }

  private void prettyPrintLineage(String column, int index, String indent, boolean start, boolean last) {
    System.out.print(indent);
    if (last) {
      System.out.print("\\-");
      indent += "  ";
    } else {
      System.out.print("|-");
      indent += "| ";
    }
    if (start) {
      System.out.println("Column: " + column);
      if (index >= 0) {
        prettyPrintLineage(column, index, indent, false, true);
      }
    } else {
      WranglerBranchingStepNode node = (WranglerBranchingStepNode) lineage.get(column).get(index--);
      System.out.println(steps.get(node.getTransformStepNumber()));
      if (node.continueUp && index >= 0) {
        prettyPrintLineage(column, index, indent, false, node.upBranches.isEmpty());
      }
      int i = 0;
      for (Map.Entry<String, Integer> entry : node.upBranches.entrySet()) {
        prettyPrintLineage(entry.getKey(), entry.getValue(), indent, true, ++i == node.upBranches.size());
      }
    }
  }

  void prettyPrint(String column, boolean forward) {
    if (forward) {
      prettyPrintFutures(column, 0, "", true, true);
    } else {
      prettyPrintLineage(column, lineage.get(column).size() - 1, "", true, true);
    }
  }

  @Override
  public List<TransformStep> getSteps() {
    return Collections.unmodifiableList(this.steps);
  }

  @Override
  public Map<String, List<BranchingTransformStepNode>> getLineage() {
    return Collections.unmodifiableMap(this.lineage);
  }

  @Override
  public String toString() {
    return "Column Directives: " + lineage + "\n" + "Steps: " + steps;
  }

  /**
   * Builder class for {@link WranglerFieldLevelLineage}.
   */
  public static class Builder {
    private static final String PARSE_KEY = "all columns";
    private static final ParseDirectiveState DEFAULT_POP = new ParseDirectiveState("random_column_%d");
    private static final Set<String> SPECIAL_DIRECTIVES = ImmutableSet.of("change-column-case",
     "cleanse-column-names", "set-headers", "columns-replace");

    private static final Comparator<ParseDirectiveState> COMPARATOR = new Comparator<ParseDirectiveState>() {
      private int percentCompare(int one, int two) {
        if (one == two) {
          return 0;
        }
        return (one == 0) ? -1 : 1;
      }

      private int lengthCompare(int one, int two) {
        if (one == two) {
          return 0;
        }
        return (one > two) ? -1 : 1;
      }

      @Override
      public int compare(ParseDirectiveState o1, ParseDirectiveState o2) {
        int firstPercentS = StringUtils.countMatches(o1.regex, "%s");
        int firstPercentD = StringUtils.countMatches(o1.regex, "%d");
        int secondPercentS = StringUtils.countMatches(o2.regex, "%s");
        int secondPercentD = StringUtils.countMatches(o2.regex, "%d");
        if (firstPercentD + firstPercentS == 0 || secondPercentD + secondPercentS == 0) {
          return percentCompare(firstPercentD + firstPercentS, secondPercentD + secondPercentS);
        }
        if (firstPercentD + firstPercentS != secondPercentD + secondPercentS) {
          return (firstPercentD + firstPercentS > secondPercentD + secondPercentS) ? -1 : 1;
        }
        if (firstPercentD != secondPercentD) {
          return (firstPercentD > secondPercentD) ? -1 : 1;
        }
        return lengthCompare(o1.regex.length() - firstPercentD - firstPercentS,
                             o2.regex.length() - secondPercentD - secondPercentS);
      }
    };

    private static final class ParseDirectiveState {
      private final List<WranglerBranchingStepNode> phantomNodes;
      private final Set<String> earlierColumns;
      private final Set<String> forbiddenColumns;

      private String regex;

      private ParseDirectiveState(String regex) {
        this.phantomNodes = new ArrayList<>();
        this.earlierColumns = new HashSet<>();
        this.forbiddenColumns = new HashSet<>();
        this.regex = regex;
      }
    }

    private int currentStepNumber;
    private Set<String> readColumns;
    private final Stack<ParseDirectiveState> lastCountingAddDirective;
    private final SortedSet<ParseDirectiveState> parseDirectives;
    private final List<String> currentColumns;
    private final List<String> endColumns;
    private final WranglerFieldLevelLineage builder;

    public Builder(List<String> startColumns, List<String> finalColumns) {
      this.currentStepNumber = -1;
      this.lastCountingAddDirective = new Stack<>();
      this.parseDirectives = new TreeSet<>(COMPARATOR);
      this.currentColumns = new ArrayList<>(startColumns);
      this.endColumns = new ArrayList<>(finalColumns);
      this.builder = new WranglerFieldLevelLineage(startColumns);
    }

    private ParseDirectiveState pop() {
      return lastCountingAddDirective.isEmpty() ? DEFAULT_POP : lastCountingAddDirective.pop();
    }

    private void create(String key, int index) {
      if (!builder.lineage.containsKey(key)) {
        builder.lineage.put(key, new ArrayList<BranchingTransformStepNode>());
      }
      if (!currentColumns.contains(key)) {
        currentColumns.add(index, key);
      }
    }

    private void create(String key) {
      create(key, currentColumns.size());
    }

    private boolean compare(String regex, String column) {
      regex = regex.replaceAll("([\\\\.\\[{}()*+?^$|])", "\\\\$1");
      regex = regex.replaceAll("%s", ".+");
      regex = regex.replaceAll("%d", "[0-9]+");
      return column.matches(regex);
    }

    private void addPhantomBranches(String key, WranglerBranchingStepNode node) {
      builder.lineage.get(key).add(node);
      for (Map.Entry<String, Integer> entry : node.downBranches.entrySet()) {
        ((WranglerBranchingStepNode) builder.lineage.get(entry.getKey()).get(entry.getValue() - 1)).putLineage(key);
      }
      for (Map.Entry<String, Integer> entry : node.upBranches.entrySet()) {
        ((WranglerBranchingStepNode) builder.lineage.get(entry.getKey()).get(entry.getValue() + 1)).putRead(key);
      }
    }

    private void addPhantomDirectives(String key, List<WranglerBranchingStepNode> phantomNodes)
      throws LineageGenerationException {
      for (WranglerBranchingStepNode node : phantomNodes) {
        addPhantomBranches(key, node);
      }
      if (phantomNodes.isEmpty()) {
        if (parseDirectives.isEmpty()) {
          throw new LineageGenerationException("Column: " + key + " does not exist.");
        }
        addPhantomBranches(key, parseDirectives.last().phantomNodes.get(0));
      }
    }

    private void addParsedColumn(String key) throws LineageGenerationException {
      if (!currentColumns.contains(key)) {
        ParseDirectiveState correctPrev = null;
        for (ParseDirectiveState curr : parseDirectives) {
          if (compare(curr.regex, key)) {
            correctPrev = curr;
            break;
          }
        }
        if (correctPrev != null) {
          int index = 0;
          for (int i = 0; i < currentColumns.size(); ++i) {
            if (correctPrev.earlierColumns.contains(currentColumns.get(i))) {
              index = i + 1;
            }
          }
          for (String column : correctPrev.forbiddenColumns) {
            if (column.compareToIgnoreCase(key) < 0) {
              if (index < currentColumns.size() && currentColumns.contains(column)) {
                ++index;
              }
            }
          }
          create(key, index);
          addPhantomDirectives(key, correctPrev.phantomNodes);
          correctPrev.forbiddenColumns.add(key);
        } else {
          throw new LineageGenerationException("Column: " + key + " does not exist.");
        }
      }
    }

    private void insertReadBranches(String key) {
      for (String readCol : readColumns) {
        if (readCol.contains(PARSE_KEY)) {
          if (readCol.equals(PARSE_KEY)) {
            for (ParseDirectiveState curr : parseDirectives) {
              curr.phantomNodes.get(curr.phantomNodes.size() - 1).putRead(key);
            }
          } else {
            String regex = readCol.split(" ")[3];
            for (ParseDirectiveState curr : parseDirectives) {
              if (curr.regex.equals(regex)) {
                curr.phantomNodes.get(curr.phantomNodes.size() - 1).putRead(key);
              }
            }
          }
        } else {
          List<BranchingTransformStepNode> curr = builder.lineage.get(readCol);
          ((WranglerBranchingStepNode) curr.get(curr.size() - 1)).putRead(key);
        }
      }
    }

    private void insertLineageBranches(WranglerBranchingStepNode node) {
      for (String readCol : readColumns) {
        if (!readCol.contains(PARSE_KEY)) {
          node.putLineage(readCol);
        }
      }
    }

    private void insertRead(String key) throws LineageGenerationException {
      addParsedColumn(key);
      builder.addBranchingStep(key, currentStepNumber, true, true);
      readColumns.add(key);
    }

    private void insertModify(String key) throws LineageGenerationException {
      addParsedColumn(key);
      WranglerBranchingStepNode node = builder.addBranchingStep(key, currentStepNumber, true, true);
      insertReadBranches(key);
      insertLineageBranches(node);
    }

    private void insertAdd(String key) {
      create(key);
      WranglerBranchingStepNode node = builder.addBranchingStep(key, currentStepNumber, false, true);
      insertReadBranches(key);
      insertLineageBranches(node);
    }

    private void insertDrop(String key) throws LineageGenerationException {
      addParsedColumn(key);
      currentColumns.remove(key);
      WranglerBranchingStepNode node = builder.addBranchingStep(key, currentStepNumber, true, false);
      insertReadBranches(key);
      insertLineageBranches(node);
    }

    private void insertRenames(List<String> keys) throws LineageGenerationException {
      WranglerBranchingStepNode node;
      BiMap<String, String> renames = HashBiMap.create(keys.size());
      for (String key : keys) {
        addParsedColumn(key.split(" ")[0]);
      }
      for (String key : keys) {
        String[] colSplit = key.split(" ");
        create(colSplit[1]);
        if (!renames.containsKey(colSplit[0]) && !renames.containsValue(colSplit[0])) {
          builder.addBranchingStep(colSplit[0], currentStepNumber, true, true);
        }
        if (!renames.containsKey(colSplit[1]) && !renames.containsValue(colSplit[1]) &&
          !colSplit[0].equals(colSplit[1])) {
          builder.addBranchingStep(colSplit[1], currentStepNumber, true, true);
        }
        renames.put(colSplit[0], colSplit[1]);
      }
      for (String key : renames.keySet()) {
        if (!renames.containsValue(key)) {
          currentColumns.remove(key);
        }
        node = builder.getLast(key);
        node.continueDown = false;
        node.putRead(renames.get(key));
      }
      for (String key : renames.values()) {
        node = builder.getLast(key);
        node.continueUp = false;
        node.putLineage(renames.inverse().get(key));
      }
    }

    private MutationDefinition fixChangeColumnCase(MutationDefinition step) {
      MutationDefinition.Builder returnVal = new MutationDefinition.Builder(step.directive());
      if (step.description().contains("lower")) {
        for (ParseDirectiveState curr : parseDirectives) {
          curr.phantomNodes.add(builder.createBranchingStep(currentStepNumber));
          curr.regex = curr.regex.toLowerCase();
          for (String column : currentColumns) {
            if (curr.earlierColumns.contains(column)) {
              curr.earlierColumns.remove(column);
              curr.earlierColumns.add(column.toLowerCase());
            }
            if (curr.forbiddenColumns.contains(column)) {
              curr.forbiddenColumns.remove(column);
              curr.forbiddenColumns.add(column.toLowerCase());
            }
          }
        }
        for (String column : currentColumns) {
          returnVal.addMutation(column + " " + column.toLowerCase(), MutationType.RENAME);
        }
      } else {
        for (ParseDirectiveState curr : parseDirectives) {
          curr.phantomNodes.add(builder.createBranchingStep(currentStepNumber));
          curr.regex = curr.regex.toUpperCase().replaceAll("%D", "%d").replaceAll("%S", "%s");
          for (String column : currentColumns) {
            if (curr.earlierColumns.contains(column)) {
              curr.earlierColumns.remove(column);
              curr.earlierColumns.add(column.toUpperCase());
            }
            if (curr.forbiddenColumns.contains(column)) {
              curr.forbiddenColumns.remove(column);
              curr.forbiddenColumns.add(column.toUpperCase());
            }
          }
        }
        for (String column : currentColumns) {
          returnVal.addMutation(column + " " + column.toUpperCase(), MutationType.RENAME);
        }
      }
      return returnVal.build();
    }

    private MutationDefinition fixCleanseColumns(MutationDefinition step) {
      MutationDefinition.Builder returnVal = new MutationDefinition.Builder(step.directive());
      for (ParseDirectiveState curr : parseDirectives) {
        curr.phantomNodes.add(builder.createBranchingStep(currentStepNumber));
        curr.regex = curr.regex.trim().toLowerCase().replaceAll("[^a-z0-9%]", "_");
        for (String column : currentColumns) {
          if (curr.earlierColumns.contains(column)) {
            curr.earlierColumns.remove(column);
            curr.earlierColumns.add(column.trim().toLowerCase().replaceAll("[^a-z0-9]", "_"));
          }
          if (curr.forbiddenColumns.contains(column)) {
            curr.forbiddenColumns.remove(column);
            curr.forbiddenColumns.add(column.trim().toLowerCase().replaceAll("[^a-z0-9]", "_"));
          }
        }
      }
      for (String column : currentColumns) {
        returnVal.addMutation(column + " " + column.trim().toLowerCase().replaceAll("[^a-z0-9]", "_"),
                              MutationType.RENAME);
      }
      return returnVal.build();
    }

    private MutationDefinition fixSetHeaders(MutationDefinition step) throws LineageGenerationException {
      int i = 0;
      for (Iterator<Mutation> columns = step.iterator(); columns.hasNext(); ++i) {
        columns.next();
      }
      int colNum = 0;
      int index = currentColumns.size();
      int gap = i - currentColumns.size();
      ParseDirectiveState curr = pop();
      int creation = curr.phantomNodes.isEmpty() ? currentStepNumber : curr.phantomNodes.get(0).stepNumber;
      for (i = 0; i < currentColumns.size(); ++i) {
        if (!builder.lineage.get(currentColumns.get(i)).isEmpty()) {
          WranglerBranchingStepNode node = (WranglerBranchingStepNode)
            builder.lineage.get(currentColumns.get(i)).get(0);
          if (!node.continueUp && node.stepNumber >= creation) {
            index = i - 1;
            break;
          }
        }
      }
      for (i = 0; i < gap; ++i) {
        String key;
        do {
          if (index < currentColumns.size()) {
            ++index;
          }
          key = curr.regex.replace("%d", Integer.toString(++colNum));
          if (currentColumns.contains(key)) {
            curr.forbiddenColumns.add(key);
          }
        }
        while (curr.forbiddenColumns.contains(key));
        create(key, index);
        addPhantomDirectives(key, curr.phantomNodes);
        curr.forbiddenColumns.add(key);
      }
      i = 0;
      MutationDefinition.Builder returnVal = new MutationDefinition.Builder(step.directive());
      for (Iterator<Mutation> columns = step.iterator(); columns.hasNext(); ++i) {
        Mutation mutation = columns.next();
        returnVal.addMutation(currentColumns.get(i) + " " + mutation.column(), MutationType.RENAME);
      }
      return returnVal.build();
    }

    private MutationDefinition fixColumnsReplace(MutationDefinition step) {
      MutationDefinition.Builder returnVal = new MutationDefinition.Builder(step.directive(),
                                                                            "Sed Expression: " + step.description());
      for (ParseDirectiveState curr : parseDirectives) {
        curr.phantomNodes.add(builder.createBranchingStep(currentStepNumber));
        int oldStart = -2;
        int newStart;
        StringBuilder newRegex = new StringBuilder();
        while ((newStart = curr.regex.indexOf('%', oldStart += 2)) != -1) {
          newRegex.append(Unix4j.echo(curr.regex.substring(oldStart, newStart))
                            .sed(step.description()).toStringResult());
          newRegex.append(curr.regex.substring(oldStart = newStart, newStart + 2));
        }
        newRegex.append(Unix4j.echo(curr.regex.substring(oldStart)).sed(step.description()).toStringResult());
        curr.regex = newRegex.toString();
        for (String column : currentColumns) {
          if (curr.earlierColumns.contains(column)) {
            curr.earlierColumns.remove(column);
            curr.earlierColumns.add(Unix4j.echo(column).sed(step.description()).toStringResult());
          }
          if (curr.forbiddenColumns.contains(column)) {
            curr.forbiddenColumns.remove(column);
            curr.forbiddenColumns.add(Unix4j.echo(column).sed(step.description()).toStringResult());
          }
        }
      }
      for (String column : currentColumns) {
        returnVal.addMutation(column + " " + Unix4j.echo(column).sed(step.description()).toStringResult(),
                              MutationType.RENAME);
      }
      return returnVal.build();
    }

    private MutationDefinition fixDirective(MutationDefinition step) throws LineageGenerationException {
      switch(step.directive()) {
        case "change-column-case":
          return fixChangeColumnCase(step);

        case "cleanse-column-names":
          return fixCleanseColumns(step);

        case "set-headers":
          return fixSetHeaders(step);

        case "columns-replace":
          return fixColumnsReplace(step);

        default:
          throw new LineageGenerationException("Unrecognized directive: " + step.directive());
      }
    }

    private List<String> parseRead(String phrase) throws LineageGenerationException {
      String[] commands = phrase.split(" ");
      List<String> list = new ArrayList<>();
      WranglerBranchingStepNode node = builder.createBranchingStep(currentStepNumber);
      if (commands.length == 2) {
        list.add(phrase);
        list.addAll(currentColumns);
        for (ParseDirectiveState curr : parseDirectives) {
          curr.phantomNodes.add(node);
        }
      } else if (commands[2].equals("formatted")) {
        list.add(phrase);
        for (String key : currentColumns) {
          if (compare(commands[3], key)) {
            list.add(key);
          }
        }
        for (ParseDirectiveState curr : parseDirectives) {
          if (curr.regex.equals(commands[3])) {
            curr.phantomNodes.add(node);
          }
        }
      } else {
        list.add("all columns");
        list.addAll(currentColumns);
        for (int i = 3; i < commands.length; ++i) {
          addParsedColumn(commands[i]);
          list.remove(commands[i]);
        }
        for (ParseDirectiveState curr : parseDirectives) {
          curr.phantomNodes.add(node);
        }
      }
      return list;
    }

    private List<String> parseModify(String phrase) throws LineageGenerationException {
      String[] commands = phrase.split(" ");
      List<String> list = new ArrayList<>();
      WranglerBranchingStepNode node = builder.createBranchingStep(currentStepNumber);
      insertLineageBranches(node);
      if (commands.length == 2) {
        list.addAll(currentColumns);
        for (ParseDirectiveState curr : parseDirectives) {
          curr.phantomNodes.add(node);
        }
      } else if (commands[2].equals("formatted")) {
        for (String key : currentColumns) {
          if (compare(commands[3], key)) {
            list.add(key);
          }
        }
        for (ParseDirectiveState curr : parseDirectives) {
          if (curr.regex.equals(commands[3])) {
            curr.phantomNodes.add(node);
          }
        }
      } else {
        list.addAll(currentColumns);
        for (int i = 3; i < commands.length; ++i) {
          addParsedColumn(commands[i]);
          list.remove(commands[i]);
        }
        for (ParseDirectiveState curr : parseDirectives) {
          curr.phantomNodes.add(node);
        }
      }
      return list;
    }

    private void parseAdd(String phrase) {
      String[] commands = phrase.split(" ");
      String regex = commands[3];
      ParseDirectiveState curr = new ParseDirectiveState(regex);
      WranglerBranchingStepNode node = builder.createBranchingStep(currentStepNumber);
      node.continueUp = false;
      insertLineageBranches(node);
      curr.phantomNodes.add(node);
      curr.earlierColumns.addAll(currentColumns);
      if (StringUtils.countMatches(regex, "%d") == 1 && StringUtils.countMatches(regex, "%s") == 0) {
        lastCountingAddDirective.push(curr);
      }
      parseDirectives.add(curr);
    }

    private List<String> parseDrop(String phrase) throws LineageGenerationException {
      String[] commands = phrase.split(" ");
      List<String> list = new ArrayList<>();
      if (commands.length == 2) {
        list.addAll(currentColumns);
        lastCountingAddDirective.clear();
        parseDirectives.clear();
      } else if (commands[2].equals("formatted")) {
        for (String key : currentColumns) {
          if (compare(commands[3], key)) {
            list.add(key);
          }
        }
        for (Iterator<ParseDirectiveState> i = lastCountingAddDirective.iterator(); i.hasNext();) {
          ParseDirectiveState curr = i.next();
          if (curr.regex.equals(commands[3])) {
            i.remove();
            break;
          }
        }
        for (Iterator<ParseDirectiveState> i = parseDirectives.iterator(); i.hasNext();) {
          ParseDirectiveState curr = i.next();
          if (curr.regex.equals(commands[3])) {
            i.remove();
          }
        }
      } else {
        list.addAll(currentColumns);
        for (int i = 3; i < commands.length; ++i) {
          addParsedColumn(commands[i]);
          list.remove(commands[i]);
        }
        lastCountingAddDirective.clear();
        parseDirectives.clear();
      }
      return list;
    }

    public WranglerFieldLevelLineage build(List<MutationDefinition> parseTree) throws LineageGenerationException {
      ((ArrayList) builder.steps).ensureCapacity(parseTree.size());
      List<String> renames = new ArrayList<>();

      for (MutationDefinition currStep : parseTree) {
        MutationType state = MutationType.READ;
        renames.clear();
        readColumns = new HashSet<>();
        ++currentStepNumber;
        if (SPECIAL_DIRECTIVES.contains(currStep.directive())) {
          currStep = fixDirective(currStep);
        }
        builder.steps.add(new WranglerTransformStep(currStep.directive(), currStep.description()));

        for (Iterator<Mutation> columns = currStep.iterator(); columns.hasNext();) {
          Mutation mutation = columns.next();
          String column = mutation.column();
          MutationType curr = mutation.type();
          if (curr == MutationType.READ && state != MutationType.READ) {
            readColumns = new HashSet<>();
          }
          if (curr != MutationType.RENAME && state == MutationType.RENAME) {
            insertRenames(renames);
            renames.clear();
          }
          state = curr;
          switch(curr) {
            case READ:
              if (column.contains(PARSE_KEY)) {
                for (String key : parseRead(column)) {
                  insertRead(key);
                }
              } else {
                insertRead(column);
              }
              break;

            case MODIFY:
              if (column.contains(PARSE_KEY)) {
                for (String key : parseModify(column)) {
                  insertModify(key);
                }
              } else {
                insertModify(column);
              }
              break;

            case ADD:
              if (column.contains(PARSE_KEY)) {
                parseAdd(column);
              } else {
                insertAdd(column);
              }
              break;

            case DROP:
              if (column.contains(PARSE_KEY)) {
                for (String key : parseDrop(column)) {
                  insertDrop(key);
                }
              } else {
                insertDrop(column);
              }
              break;

            case RENAME:
              renames.add(column);
              break;

            default:
              throw new LineageGenerationException("Unrecognized label on column: " + column);
          }
        }
        if (state == MutationType.RENAME) {
          insertRenames(renames);
        }
      }
      for (String key : endColumns) {
        addParsedColumn(key);
      }
      return builder;
    }
  }
}
