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

package io.cdap.wrangler.store.upgrade;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.test.SystemAppTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Upgrade store test
 */
public class UpgradeStoreTest extends SystemAppTestBase {
  private static UpgradeStore store;

  @BeforeClass
  public static void setupTest() throws Exception {
    getStructuredTableAdmin().create(UpgradeStore.UPGRADE_TABLE_SPEC);
    store = new UpgradeStore(getTransactionRunner());
  }

  @After
  public void cleanupTest() throws Exception {
    store.clear();
  }

  @Test
  public void testUpgradeTimestampDoesNotChange() throws Exception {
    long tsNow = System.currentTimeMillis();
    long upgradeTs = store.initializeAndRetrieveUpgradeTimestampMillis(
      UpgradeEntityType.CONNECTION, tsNow, new UpgradeState(0L));
    Assert.assertTrue(upgradeTs > 0);
    // wait for time to pass at least 1 milli second
    Tasks.waitFor(true, () -> System.currentTimeMillis() > upgradeTs, 5, TimeUnit.MILLISECONDS);
    long actual = store.initializeAndRetrieveUpgradeTimestampMillis(
      UpgradeEntityType.CONNECTION, System.currentTimeMillis(), new UpgradeState(1L));
    Assert.assertEquals(upgradeTs, actual);
    Assert.assertEquals(new UpgradeState(0L), store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
  }

  @Test
  public void testUpgradeStore() throws Exception {
    List<NamespaceSummary> namespaces = ImmutableList.of(
      new NamespaceSummary("default", "", 0L),
      new NamespaceSummary("test1", "", 1L),
      new NamespaceSummary("test2", "", 0L),
      new NamespaceSummary("test3", "", 5L));

    Assert.assertNull(store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
    Assert.assertNull(store.getEntityUpgradeState(UpgradeEntityType.WORKSPACE));

    UpgradeState preUpgrade = new UpgradeState(0L);
    store.initializeAndRetrieveUpgradeTimestampMillis(UpgradeEntityType.CONNECTION,
                                                      System.currentTimeMillis(), preUpgrade);
    store.initializeAndRetrieveUpgradeTimestampMillis(UpgradeEntityType.WORKSPACE,
                                                      System.currentTimeMillis(), preUpgrade);
    Assert.assertEquals(preUpgrade, store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
    Assert.assertEquals(preUpgrade, store.getEntityUpgradeState(UpgradeEntityType.WORKSPACE));

    UpgradeState upgraded = new UpgradeState(1L);

    // assert connection upgrade completion
    namespaces.forEach(ns -> {
      store.setEntityUpgradeState(ns, UpgradeEntityType.CONNECTION, upgraded);
      Assert.assertEquals(upgraded, store.getEntityUpgradeState(ns, UpgradeEntityType.CONNECTION));
      Assert.assertEquals(preUpgrade, store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
      Assert.assertEquals(preUpgrade, store.getEntityUpgradeState(UpgradeEntityType.WORKSPACE));
    });

    // connection upgrade is done
    store.setEntityUpgradeState(UpgradeEntityType.CONNECTION, upgraded);
    Assert.assertEquals(upgraded, store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
    Assert.assertEquals(preUpgrade, store.getEntityUpgradeState(UpgradeEntityType.WORKSPACE));

    // assert workspace upgrade completion
    namespaces.forEach(ns -> {
      store.setEntityUpgradeState(ns, UpgradeEntityType.WORKSPACE, upgraded);
      Assert.assertEquals(upgraded, store.getEntityUpgradeState(ns, UpgradeEntityType.WORKSPACE));
      Assert.assertEquals(preUpgrade, store.getEntityUpgradeState(UpgradeEntityType.WORKSPACE));
      Assert.assertEquals(upgraded, store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
    });

    // workspace upgrade is done
    store.setEntityUpgradeState(UpgradeEntityType.WORKSPACE, upgraded);

    // upgrade is done
    Assert.assertEquals(upgraded, store.getEntityUpgradeState(UpgradeEntityType.CONNECTION));
    Assert.assertEquals(upgraded, store.getEntityUpgradeState(UpgradeEntityType.WORKSPACE));
  }
}
