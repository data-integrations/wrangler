/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.wrangler.service.macro;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;

import java.util.Map;

/**
 * Dataprep Service macro evaluator.
 */
public class ServiceMacroEvaluator implements MacroEvaluator {
  private static final String SECURE_FUNCTION = "secure";
  private final String namespace;
  private final SystemHttpServiceContext context;
  private final Map<String, String> arguments;

  public ServiceMacroEvaluator(String namespace, SystemHttpServiceContext context) {
    this.namespace = namespace;
    this.context = context;
    this.arguments = context.getRuntimeArguments();
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    String val = arguments.get(property);
    if (val == null) {
      throw new InvalidMacroException(String.format("Argument '%s' is not defined.", property));
    }
    return val;
  }

  @Override
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    if (!SECURE_FUNCTION.equals(macroFunction)) {
      throw new InvalidMacroException(String.format("%s is not a supported macro function.", macroFunction));
    }
    if (arguments.length != 1) {
      throw new InvalidMacroException("Secure macro function only supports 1 argument.");
    }
    try {
      SecureStoreData secureStoreData = context.get(namespace, arguments[0]);
      return Bytes.toString(secureStoreData.get());
    } catch (Exception e) {
      throw new InvalidMacroException("Failed to resolve macro '" + macroFunction + "(" + arguments[0] + ")'", e);
    }
  }
}
