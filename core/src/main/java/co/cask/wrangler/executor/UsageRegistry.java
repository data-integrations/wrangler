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

package co.cask.wrangler.executor;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.wrangler.api.AbstractStep;
import co.cask.wrangler.api.Usage;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Registry of directive usages managed through this class.
 */
public final class UsageRegistry implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(UsageRegistry.class);

  public class UsageDatum {
    private final String directive;
    private final String usage;
    private final String description;

    public UsageDatum(String directive, String usage, String description) {
      this.directive = directive;
      this.usage = usage;
      this.description = description;
    }

    public String getDirective() {
      return directive;
    }

    public String getUsage() {
      return usage;
    }

    public String getDescription() {
      return description;
    }
  }

  // Mapping of specification usages.
  private final Map<String, UsageDatum> usages = new HashMap<>();

  // Listing.
  private final List<UsageDatum> usageList = new ArrayList<>();

  public UsageRegistry() {
    // Iterate through registry of steps to collect the
    // directive and usage.
    Reflections reflections = new Reflections("co.cask.wrangler");
    Set<Class<? extends AbstractStep>> steps = reflections.getSubTypesOf(AbstractStep.class);
    for (Class<? extends AbstractStep> step : steps) {
      Name name = step.getAnnotation(Name.class);
      Description description = step.getAnnotation(Description.class);
      Usage usage = step.getAnnotation(Usage.class);
      if (usage == null || name == null || description == null) {
        LOG.warn("Usage or Name or Description annotation for directive '{}' missing.", step.getSimpleName());
        continue;
      }
      usages.put(name.value(), new UsageDatum(name.value(), usage.value(), description.value()));
      usageList.add(new UsageDatum(name.value(), usage.value(), description.value()));
    }

    // These are for directives that use other steps for executing.
    // we add them exclusively
    addUsage("set format", "set format csv <delimiter> <skip empty lines>",
             "[DEPRECATED] Parses the predefined column as CSV. Use 'parse-as-csv' instead.");
    addUsage("format-unix-timestamp", "format-unix-timestamp <column> <format>",
             "Formats a UNIX timestamp using the specified format");
    addUsage("filter-row-if-not-matched", "filter-row-if-not-matched <column> <regex>",
             "Filters rows if the regex does not match");
    addUsage("filter-row-if-false", "filter-row-if-false <condition>",
             "Filters rows if the condition evaluates to false");
  }

  private void addUsage(String directive, String usage, String description) {
    UsageDatum d = new UsageDatum(directive, usage, description);
    usages.put(directive, d);
    usageList.add(d);
  }

  /**
   * Gets the usage of a directive.
   *
   * @param directive for which usage is returned.
   * @return null if not found, else the usage.
   */
  public String getUsage(String directive) {
    if(usages.containsKey(directive)) {
      return usages.get(directive).getUsage();
    }
    return null;
  }

  /**
   * Gets the description of a directive.
   *
   * @param directive for which usage is returned.
   * @return null if not found, else the description of usage.
   */
  public String getDescription(String directive) {
    if(usages.containsKey(directive)) {
      return usages.get(directive).getDescription();
    }
    return null;
  }

  /**
   * @return A map of directive to {@link UsageDatum}.
   */
  public List<UsageDatum> getAll() {
    return usageList;
  }
}
