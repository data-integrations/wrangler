package co.cask.wrangler.api;

import co.cask.wrangler.internal.UsageRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests {@link UsageRegistry}
 */
public class UsageRegistryTest {

  @Test
  public void testUsageRegistry() throws Exception {
    UsageRegistry registry = new UsageRegistry();
    Map<String, UsageRegistry.UsageDatum> usages = registry.getAll();
    Assert.assertTrue(usages.size() > 1);
  }
}
