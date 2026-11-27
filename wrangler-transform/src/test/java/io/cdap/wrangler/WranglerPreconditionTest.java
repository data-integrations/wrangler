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

package io.cdap.wrangler;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.mock.common.MockArguments;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import io.cdap.cdap.features.Feature;
import io.cdap.wrangler.api.DefaultJexlAllowlist;
import io.cdap.wrangler.api.DirectiveConfig;
import io.cdap.wrangler.api.JexlConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests JEXL allowlist enforcement for {@link Precondition} through {@link Wrangler}.
 */
public class WranglerPreconditionTest {

  private static final Gson GSON = new Gson();
  private static final Schema SCHEMA = Schema.recordOf(
      "record",
      Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  @Test
  public void testDisallowedPreconditionFailsPipelineForAllOnErrorStrategies() throws Exception {
    String exploit =
        "body.getClass().forName(\"java.lang.Runtime\").getMethod(\"getRuntime\").invoke(null) != null";
    for (String onError : ImmutableList.of("fail-pipeline", "skip-error", "send-to-error-port")) {
      Wrangler wrangler = createAndInitWrangler(
          exploit, onError, /* allowlistConfigEnabled= */ true, /* featureFlagEnabled= */ true);
      MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
      StructuredRecord record = StructuredRecord.builder(SCHEMA).set("body", "hello").build();
      try {
        wrangler.transform(record, emitter);
        Assert.fail("Expected ProgramFailureException for on-error=" + onError);
      } catch (ProgramFailureException e) {
        Assert.assertTrue(
            "Expected ELPermissionException in causal chain for on-error=" + onError,
            WranglerErrorUtil.isCriticalException(e));
        Assert.assertTrue(emitter.getEmitted().isEmpty());
        Assert.assertTrue(emitter.getErrors().isEmpty());
      } finally {
        wrangler.destroy();
      }
    }
  }

  @Test
  public void testAllowlistedPreconditionFiltersRecordsWhenSandboxEnabled() throws Exception {
    Wrangler wrangler = createAndInitWrangler(
        "body.startsWith(\"skip\")", "fail-pipeline",
        /* allowlistConfigEnabled= */ true, /* featureFlagEnabled= */ true);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    try {
      wrangler.transform(StructuredRecord.builder(SCHEMA).set("body", "skip-me").build(), emitter);
      wrangler.transform(StructuredRecord.builder(SCHEMA).set("body", "keep-me").build(), emitter);
      Assert.assertEquals(1, emitter.getEmitted().size());
      Assert.assertEquals("keep-me", emitter.getEmitted().get(0).get("body"));
    } finally {
      wrangler.destroy();
    }
  }

  @Test
  public void testPreconditionSandboxDisabledWhenFeatureFlagOff() throws Exception {
    Wrangler wrangler = createAndInitWrangler(
        "new('java.lang.Object').getClass().getName() == 'other'", "fail-pipeline",
        /* allowlistConfigEnabled= */ true, /* featureFlagEnabled= */ false);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    try {
      wrangler.transform(StructuredRecord.builder(SCHEMA).set("body", "hello").build(), emitter);
      Assert.assertEquals(1, emitter.getEmitted().size());
      Assert.assertEquals("hello", emitter.getEmitted().get(0).get("body"));
    } finally {
      wrangler.destroy();
    }
  }

  @Test
  public void testPreconditionSandboxDisabledWhenConfigDisabled() throws Exception {
    Wrangler wrangler = createAndInitWrangler(
        "new('java.lang.Object').getClass().getName() == 'other'", "fail-pipeline",
        /* allowlistConfigEnabled= */ false, /* featureFlagEnabled= */ true);
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();
    try {
      wrangler.transform(StructuredRecord.builder(SCHEMA).set("body", "hello").build(), emitter);
      Assert.assertEquals(1, emitter.getEmitted().size());
      Assert.assertEquals("hello", emitter.getEmitted().get(0).get("body"));
    } finally {
      wrangler.destroy();
    }
  }

  @Test
  public void testNonPermissionPreconditionErrorHonorsOnErrorStrategies() throws Exception {
    StructuredRecord record = StructuredRecord.builder(SCHEMA).set("body", "hello").build();

    Wrangler skipWrangler = createAndInitWrangler(
        "missingCol == 'x'", "skip-error",
        /* allowlistConfigEnabled= */ true, /* featureFlagEnabled= */ true);
    MockEmitter<StructuredRecord> skipEmitter = new MockEmitter<>();
    try {
      skipWrangler.transform(record, skipEmitter);
      Assert.assertTrue(skipEmitter.getEmitted().isEmpty());
      Assert.assertTrue(skipEmitter.getErrors().isEmpty());
    } finally {
      skipWrangler.destroy();
    }

    Wrangler errorPortWrangler = createAndInitWrangler(
        "missingCol == 'x'", "send-to-error-port",
        /* allowlistConfigEnabled= */ true, /* featureFlagEnabled= */ true);
    MockEmitter<StructuredRecord> errorEmitter = new MockEmitter<>();
    try {
      errorPortWrangler.transform(record, errorEmitter);
      Assert.assertTrue(errorEmitter.getEmitted().isEmpty());
      Assert.assertEquals(1, errorEmitter.getErrors().size());
    } finally {
      errorPortWrangler.destroy();
    }
  }

  private static Wrangler createAndInitWrangler(
      String precondition,
      String onError,
      boolean allowlistConfigEnabled,
      boolean featureFlagEnabled) throws Exception {
    Wrangler.Config config = new Wrangler.Config(
        "jexl", precondition, "", null, "*", SCHEMA.toString(), onError);
    Wrangler wrangler = new Wrangler(config);
    DirectiveConfig directiveConfig = new DirectiveConfig(
        null, null, new JexlConfiguration(allowlistConfigEnabled, DefaultJexlAllowlist.get()));
    MockArguments arguments = new MockArguments(
        Collections.singletonMap("wrangler.directive.config", GSON.toJson(directiveConfig)));
    wrangler.initialize(new FeatureFlagTransformContext(arguments, featureFlagEnabled));
    return wrangler;
  }

  private static final class FeatureFlagTransformContext extends MockTransformContext {
    private final Arguments arguments;
    private final boolean allowlistFeatureEnabled;

    private FeatureFlagTransformContext(Arguments arguments, boolean allowlistFeatureEnabled) {
      super("wrangler");
      this.arguments = arguments;
      this.allowlistFeatureEnabled = allowlistFeatureEnabled;
    }

    @Override
    public String getNamespace() {
      return "default";
    }

    @Override
    public Arguments getArguments() {
      return arguments;
    }

    @Override
    public boolean isFeatureEnabled(String name) {
      return Feature.WRANGLER_JEXL_ALLOWLIST.getFeatureFlagString().equals(name)
          && allowlistFeatureEnabled;
    }
  }
}
