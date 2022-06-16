/*
 * Copyright © 2021 Cask Data, Inc.
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
 *
 */

package io.cdap.wrangler.service.directive;

import io.cdap.wrangler.api.TokenGroup;
import io.cdap.wrangler.parser.GrammarWalker;
import io.cdap.wrangler.registry.DirectiveInfo;
import io.cdap.wrangler.registry.SystemDirectiveRegistry;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * This Visitor collects user directives and helps to add #pragma directive to load them to the
 * script
 */
public class UserDirectivesCollector implements GrammarWalker.Visitor<RuntimeException> {
  private static final Set<String> userDirectives = new LinkedHashSet<>();

  @Override
  public void visit(String command, TokenGroup tokenGroup) {
    DirectiveInfo info = SystemDirectiveRegistry.INSTANCE.get(command);
    if (info == null) {
      userDirectives.add(command);
    }
  }

  /**
   * If any user directives was found, adds load-directives pragma as the first command.
   */
  public void addLoadDirectivesPragma(List<String> directives) {
    if (!userDirectives.isEmpty()) {
      directives.add(0, "#pragma load-directives " + String.join(",", userDirectives) + ";");
    }
  }
}
