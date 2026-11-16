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

package io.cdap.wrangler.service.adls;

import io.cdap.wrangler.utils.OutboundUrlValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for {@link ADLSConfiguration} SSRF validation.
 */
public class ADLSConfigurationTest {

  @Before
  public void setUp() {
    System.clearProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY);
  }

  @After
  public void tearDown() {
    System.clearProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY);
  }

  @Test
  public void testValidExternalAdlsConfiguration() {
    Map<String, String> props = new HashMap<>();
    props.put("clientID", "test-client-id");
    props.put("clientSecret", "test-secret");
    props.put("refreshURL", "https://93.184.216.34/tenant/oauth2/token");
    props.put("accountFQDN", "93.184.216.34");

    ADLSConfiguration config = new ADLSConfiguration(props);
    Assert.assertEquals("test-client-id", config.getADLSClientID());
    Assert.assertEquals("https://93.184.216.34/tenant/oauth2/token", config.getEndpointURL());
    Assert.assertEquals("93.184.216.34", config.getAccountFQDN());
  }

  @Test
  public void testBlockedMetadataRefreshUrlRejected() {
    String[] blockedRefreshUrls = new String[] {
      "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
      "http://metadata.google.internal/computeMetadata/v1/",
      "http://localhost:8080/token",
      "http://10.0.0.1/token",
      "file:///etc/passwd"
    };
    for (String refreshUrl : blockedRefreshUrls) {
      Map<String, String> props = new HashMap<>();
      props.put("clientID", "test-client-id");
      props.put("clientSecret", "test-secret");
      props.put("refreshURL", refreshUrl);
      props.put("accountFQDN", "93.184.216.34");
      try {
        new ADLSConfiguration(props);
        Assert.fail("Expected ADLSConfiguration to reject refreshURL: " + refreshUrl);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testBlockedInternalAccountFqdnRejected() {
    String[] blockedFqdns = new String[] {
      "169.254.169.254",
      "metadata.google.internal",
      "localhost",
      "127.0.0.1",
      "kubernetes.default.svc.cluster.local",
      "10.0.0.1",
      "https://93.184.216.34",
      "93.184.216.34:443",
      "93.184.216.34/path"
    };
    for (String fqdn : blockedFqdns) {
      Map<String, String> props = new HashMap<>();
      props.put("clientID", "test-client-id");
      props.put("clientSecret", "test-secret");
      props.put("refreshURL", "https://93.184.216.34/tenant/oauth2/token");
      props.put("accountFQDN", fqdn);
      try {
        new ADLSConfiguration(props);
        Assert.fail("Expected ADLSConfiguration to reject accountFQDN: " + fqdn);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }
}
