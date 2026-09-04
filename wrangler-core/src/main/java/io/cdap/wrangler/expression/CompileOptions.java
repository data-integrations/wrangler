/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.wrangler.expression;

import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.DirectiveContext;
import io.cdap.wrangler.api.JexlAllowlist;
import io.cdap.wrangler.parser.MapArgumentsWithContext;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Holds options used during JEXL script compilation.
 */
public class CompileOptions {
  private static final CompileOptions DEFAULT = new CompileOptions(false, null);

  private final boolean allowlistEnabled;
  private final List<JexlAllowlist> jexlAllowlist;

  private CompileOptions(boolean allowlistEnabled, @Nullable List<JexlAllowlist> jexlAllowlist) {
    this.allowlistEnabled = allowlistEnabled;
    this.jexlAllowlist = jexlAllowlist;
  }

  public static CompileOptions getDefaultCompileOptions() {
    return DEFAULT;
  }

  public boolean isAllowlistEnabled() {
    return allowlistEnabled;
  }

  public List<JexlAllowlist> getJexlAllowlist() {
    return jexlAllowlist;
  }

  public static CompileOptions fromArguments(Arguments args) {
    if (args instanceof MapArgumentsWithContext) {
      DirectiveContext context = ((MapArgumentsWithContext) args).getDirectiveContext();
      if (context != null) {
        return new CompileOptions(context.isJexlAllowlistEnabled(), context.getJexlAllowlist());
      }
    }
    return DEFAULT;
  }
}
