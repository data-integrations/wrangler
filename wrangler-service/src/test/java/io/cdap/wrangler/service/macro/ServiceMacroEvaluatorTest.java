/*
 * Copyright © 2026 Cask Data, Inc.
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
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.api.security.store.SecureStoreMetadata;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.internal.app.runtime.plugin.MacroParser;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.Map;

/**
 * Unit tests for {@link ServiceMacroEvaluator}.
 */
public class ServiceMacroEvaluatorTest {

  @Test
  public void testEvaluateDoesNotReflectArgumentOrCause() {
    String secretValue = "super-secret-value-123";
    SystemHttpServiceContext context = createContext(Collections.emptyMap(), Collections.emptyMap());

    ServiceMacroEvaluator evaluator = new ServiceMacroEvaluator("default", context);
    try {
      evaluator.evaluate("secure", secretValue);
      Assert.fail("Expected InvalidMacroException");
    } catch (InvalidMacroException e) {
      Assert.assertEquals("Failed to resolve macro 'secure'.", e.getMessage());
      Assert.assertNull(e.getCause());
    }
  }

  @Test
  public void testLookupDoesNotReflectPropertyName() {
    String secretValue = "super-secret-value-123";
    SystemHttpServiceContext context = createContext(Collections.emptyMap(), Collections.emptyMap());

    ServiceMacroEvaluator evaluator = new ServiceMacroEvaluator("default", context);
    try {
      evaluator.lookup(secretValue);
      Assert.fail("Expected InvalidMacroException");
    } catch (InvalidMacroException e) {
      Assert.assertEquals("Failed to resolve macro argument.", e.getMessage());
      Assert.assertNull(e.getCause());
    }
  }

  @Test
  public void testUnsupportedFunctionDoesNotReflectFunctionName() {
    String secretValue = "super-secret-value-123";
    SystemHttpServiceContext context = createContext(Collections.emptyMap(), Collections.emptyMap());

    ServiceMacroEvaluator evaluator = new ServiceMacroEvaluator("default", context);
    try {
      evaluator.evaluate(secretValue, "arg");
      Assert.fail("Expected InvalidMacroException");
    } catch (InvalidMacroException e) {
      Assert.assertEquals("Unsupported macro function.", e.getMessage());
      Assert.assertNull(e.getCause());
    }
  }

  @Test
  public void testNestedSecureMacrosDoNotLeakSecretThroughMacroParser() {
    String secretValue = "super-secret-value-123";
    SystemHttpServiceContext context = createContext(
      Collections.emptyMap(), Collections.singletonMap("my-key", secretValue));
    ServiceMacroEvaluator evaluator = new ServiceMacroEvaluator("default", context);
    MacroParserOptions options = MacroParserOptions.builder()
      .setFunctionWhitelist("secure")
      .setEscaping(false)
      .build();
    MacroParser parser = new MacroParser(evaluator, options);

    try {
      parser.parse("${secure(${secure(my-key)})}");
      Assert.fail("Expected InvalidMacroException");
    } catch (InvalidMacroException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertFalse(e.getMessage().contains(secretValue));
      Assert.assertNull(e.getCause());
    }

    try {
      parser.parse("${${secure(my-key)}}");
      Assert.fail("Expected InvalidMacroException");
    } catch (InvalidMacroException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertFalse(e.getMessage().contains(secretValue));
      Assert.assertNull(e.getCause());
    }
  }

  @Test
  public void testEvaluateResolvesValidSecureKey() {
    SystemHttpServiceContext context = createContext(
      Collections.emptyMap(), Collections.singletonMap("my-key", "resolved-val"));

    ServiceMacroEvaluator evaluator = new ServiceMacroEvaluator("default", context);
    Assert.assertEquals("resolved-val", evaluator.evaluate("secure", "my-key"));
  }

  private static SystemHttpServiceContext createContext(Map<String, String> runtimeArgs,
                                                        Map<String, String> secureStore) {
    return (SystemHttpServiceContext) Proxy.newProxyInstance(
      SystemHttpServiceContext.class.getClassLoader(),
      new Class<?>[]{SystemHttpServiceContext.class},
      (proxy, method, args) -> {
        if ("getRuntimeArguments".equals(method.getName())) {
          return runtimeArgs;
        }
        if ("get".equals(method.getName()) && args != null && args.length == 2) {
          String key = (String) args[1];
          String val = secureStore.get(key);
          if (val == null) {
            throw new IllegalStateException("Key " + args[0] + ":" + key + " not found");
          }
          return new SecureStoreData(
            new SecureStoreMetadata(key, "", 0L, Collections.emptyMap()), Bytes.toBytes(val));
        }
        return null;
      });
  }
}
