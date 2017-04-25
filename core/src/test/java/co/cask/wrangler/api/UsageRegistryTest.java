package co.cask.wrangler.api;

import co.cask.wrangler.executor.UsageRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link UsageRegistry}
 */
public class UsageRegistryTest {

  @Test
  public void testUsageRegistry() throws Exception {
    UsageRegistry registry = new UsageRegistry();
    List<UsageRegistry.UsageDatum> usages = registry.getAll();
    Assert.assertTrue(usages.size() > 1);
  }
}
