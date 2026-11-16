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

package io.cdap.wrangler.utils;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link OutboundUrlValidator}.
 */
public class OutboundUrlValidatorTest {

  @Before
  public void setUp() {
    System.clearProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY);
    OutboundUrlValidator.resetHostResolverForTesting();
  }

  @After
  public void tearDown() {
    System.clearProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY);
    OutboundUrlValidator.resetHostResolverForTesting();
  }

  @Test
  public void testAllowedPublicUrls() throws Exception {
    OutboundUrlValidator.setHostResolverForTesting(host -> new InetAddress[] {
      InetAddress.getByAddress(host, new byte[] {(byte) 93, (byte) 184, (byte) 216, 34})
    });

    URI httpUri = OutboundUrlValidator.validateOutboundUrl("http://example.com/api/v1/resource");
    Assert.assertEquals("http", httpUri.getScheme());
    Assert.assertEquals("example.com", httpUri.getHost());

    URI httpsUri = OutboundUrlValidator.validateOutboundUrl("HTTPS://api.example.com:8443/v2/models");
    Assert.assertEquals("HTTPS", httpsUri.getScheme());
    Assert.assertEquals("api.example.com", httpsUri.getHost());
    Assert.assertEquals(8443, httpsUri.getPort());

    // Reset the host resolver so IP literal assertions below test actual IP parsing
    OutboundUrlValidator.resetHostResolverForTesting();

    // Public IPv4, IPv6, and port boundary values around blocked ranges
    String[] allowedPublicIps = new String[] {
      "http://93.184.216.34:1/min-port",
      "http://93.184.216.34:65535/max-port",
      "http://1.0.0.1/boundary",
      "http://9.255.255.255/boundary",
      "http://11.0.0.1/boundary",
      "http://93.184.216.34/data",
      "https://8.8.8.8/dns",
      "http://100.63.255.255/boundary",
      "http://100.128.0.1/boundary",
      "http://126.255.255.255/boundary",
      "http://128.0.0.1/boundary",
      "http://169.253.255.255/boundary",
      "http://169.255.0.1/boundary",
      "http://172.15.255.255/boundary",
      "http://172.32.0.1/boundary",
      "http://192.0.1.1/boundary",
      "http://192.167.255.255/boundary",
      "http://192.169.0.1/boundary",
      "http://198.17.255.255/boundary",
      "http://198.20.0.1/boundary",
      "http://223.255.255.254/boundary",
      "http://[2001:4860:4860::8888]/dns6",
      "http://[64:ff9b::93.184.216.34]/nat64-public"
    };
    for (String url : allowedPublicIps) {
      Assert.assertNotNull("Expected public URL to be allowed: " + url,
                           OutboundUrlValidator.validateOutboundUrl(url));
    }
  }

  @Test
  public void testDisallowedSchemesAndSyntax() {
    String[] invalidUrls = new String[] {
      null,
      "",
      "   ",
      "file:///etc/passwd",
      "FILE:///var/run/secrets/kubernetes.io/serviceaccount/token",
      "jar:file:/tmp/archive.jar!/manifest.json",
      "ftp://93.184.216.34/pub/file.txt",
      "gopher://93.184.216.34:70/_",
      "netdoc:///etc/hosts",
      "javascript:alert(1)",
      "data:text/plain;base64,SGVsbG8=",
      "http://user:password@93.184.216.34/path",
      "http://user@93.184.216.34/path",
      "http://example.com@169.254.169.254/computeMetadata/v1/",
      "http://93.184.216.34:0/path",
      "http://93.184.216.34:65536/path",
      "http:///path-without-host",
      "http://93.184.216.34\r\nX-Injected: true"
    };
    for (String url : invalidUrls) {
      assertRejectedUrl(url);
    }
  }

  @Test
  public void testBlockedInternalAndMetadataHostnames() {
    OutboundUrlValidator.setHostResolverForTesting(host -> new InetAddress[] {
      InetAddress.getByAddress(host, new byte[] {(byte) 93, (byte) 184, (byte) 216, 34})
    });
    String[] blockedHostUrls = new String[] {
      "http://localhost/api",
      "http://localhost:8080/api",
      "http://localhost./api",
      "http://sub.localhost/api",
      "http://ip6-localhost/api",
      "http://ip6-loopback/api",
      "http://metadata/computeMetadata/v1/",
      "http://metadata.google.internal/computeMetadata/v1/",
      "http://metadata.google.internal./computeMetadata/v1/",
      "http://metadata\u3002google\u3002internal/computeMetadata/v1/",
      "http://METADATA.GOOGLE.INTERNAL/computeMetadata/v1/",
      "http://sub.metadata.google.internal/computeMetadata/v1/",
      "http://metadata.goog/computeMetadata/v1/",
      "http://service.internal/v1",
      "http://node.local/v1",
      "http://cdap-master.default.svc.cluster.local:11015/v3/namespaces",
      "http://cdap-master.default.svc:11015/v3/namespaces",
      "http://my-pod.default.pod/status",
      "http://kubernetes/api",
      "http://kubernetes.default/api",
      "http://kubernetes.default.svc/api",
      "http://kubernetes.default.svc.cluster.local/api"
    };
    for (String url : blockedHostUrls) {
      assertRejectedUrl(url);
    }
  }

  @Test
  public void testBlockedNonCanonicalIpv4Encodings() {
    String[] obfuscatedIpUrls = new String[] {
      "http://0177.0.0.1/",
      "http://0251.0376.0251.0376/",
      "http://0x7f.0.0.1/",
      "http://0xa9.0xfe.0xa9.0xfe/",
      "http://127.0.0.0x1/",
      "http://2130706433/",
      "http://2852039166/",
      "http://127.1/",
      "http://169.254.43518/",
      "http://10.1.1/"
    };
    for (String url : obfuscatedIpUrls) {
      assertRejectedUrl(url);
    }
  }

  @Test
  public void testBlockedIpv4Ranges() {
    String[] blockedIpv4Urls = new String[] {
      "http://0.0.0.0/",
      "http://0.1.2.3/",
      "http://127.0.0.1/",
      "http://127.0.0.2:8080/",
      "http://127.255.255.254/",
      "http://10.0.0.1/",
      "http://10.255.255.254/",
      "http://172.16.0.1/",
      "http://172.31.255.254/",
      "http://192.168.0.1/",
      "http://192.168.255.254/",
      "http://169.254.169.254/computeMetadata/v1/",
      "http://169.254.0.1/",
      "http://169.254.255.254/",
      "http://100.64.0.1/",
      "http://100.100.100.200/latest/meta-data/",
      "http://100.127.255.254/",
      "http://192.0.0.1/",
      "http://192.0.2.1/",
      "http://198.18.0.1/",
      "http://198.19.255.254/",
      "http://198.51.100.1/",
      "http://203.0.113.1/",
      "http://224.0.0.1/",
      "http://239.255.255.250/",
      "http://240.0.0.1/",
      "http://255.255.255.255/"
    };
    for (String url : blockedIpv4Urls) {
      assertRejectedUrl(url);
    }
  }

  @Test
  public void testBlockedIpv6AndTransitionRanges() {
    String[] blockedIpv6Urls = new String[] {
      "http://[::]/",
      "http://[::1]/",
      "http://[0:0:0:0:0:0:0:1]:8080/",
      "http://[fe80::1]/",
      "http://[febf::1]/",
      "http://[fe80::1%25eth0]/",
      "http://[fc00::1]/",
      "http://[fd12:3456:789a::1]/",
      "http://[fd00:ec2::254]/latest/meta-data/",
      "http://[fec0::1]/",
      "http://[ff02::1]/",
      "http://[2001:db8::1]/",
      "http://[100::1]/",
      // IPv4-mapped IPv6
      "http://[::ffff:169.254.169.254]/computeMetadata/v1/",
      "http://[::ffff:127.0.0.1]/",
      "http://[::ffff:10.0.0.1]/",
      "http://[::ffff:172.16.0.1]/",
      "http://[::ffff:192.168.1.1]/",
      // IPv4-compatible IPv6
      "http://[::169.254.169.254]/",
      "http://[::10.0.0.1]/",
      // SIIT IPv4-translated
      "http://[::ffff:0:169.254.169.254]/",
      // NAT64 well-known and local-use prefixes
      "http://[64:ff9b::169.254.169.254]/",
      "http://[64:ff9b:1::169.254.169.254]/",
      "http://[64:ff9b::10.0.0.1]/",
      // 6to4 embedding 169.254.169.254 (a9fe:a9fe) and 10.0.0.1 (0a00:0001)
      "http://[2002:a9fe:a9fe::1]/",
      "http://[2002:0a00:0001::1]/",
      // Teredo embedding client 169.254.169.254 (XOR 0xff -> 5601:5601) or blocked Teredo server
      "http://[2001:0000:4136:e378:8000:63bf:5601:5601]/",
      "http://[2001:0000:a9fe:a9fe:8000:63bf:a247:27dd]/",
      // ISATAP embedding 169.254.169.254 (both 0000:5efe and 0200:5efe)
      "http://[2001:470::5efe:169.254.169.254]/",
      "http://[2001:470::200:5efe:169.254.169.254]/"
    };
    for (String url : blockedIpv6Urls) {
      assertRejectedUrl(url);
    }
  }

  @Test
  public void testMultiRecordDnsAndRebindingRejected() throws Exception {
    OutboundUrlValidator.setHostResolverForTesting(host -> new InetAddress[] {
      InetAddress.getByAddress(host, new byte[] {(byte) 93, (byte) 184, (byte) 216, 34}),
      InetAddress.getByAddress(host, new byte[] {(byte) 169, (byte) 254, (byte) 169, (byte) 254})
    });
    assertRejectedUrl("http://rebind.example.com/data");

    OutboundUrlValidator.setHostResolverForTesting(host -> new InetAddress[] {
      InetAddress.getByAddress(host, new byte[] {(byte) 93, (byte) 184, (byte) 216, 34}),
      InetAddress.getByAddress(host, new byte[] {10, 0, 0, 1})
    });
    assertRejectedUrl("http://mixed.example.com/data");

    OutboundUrlValidator.setHostResolverForTesting(host -> {
      throw new UnknownHostException(host);
    });
    assertRejectedUrl("http://unresolvable.example.com/data");
  }

  @Test
  public void testPrivateNetworksOptInStillBlocksMetadataAndClusterHosts() {
    System.setProperty(OutboundUrlValidator.ALLOW_PRIVATE_NETWORKS_PROPERTY, "true");
    OutboundUrlValidator.setHostResolverForTesting(host -> new InetAddress[] {
      InetAddress.getByAddress(host, new byte[] {127, 0, 0, 1})
    });

    // Loopback, RFC 1918, CGNAT, and IPv6 ULA are allowed when explicitly opted in
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://localhost:8080/service"));
    OutboundUrlValidator.resetHostResolverForTesting();
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://127.0.0.1:8080/service"));
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://10.0.0.15:8080/service"));
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://172.16.5.4:8080/service"));
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://192.168.1.10:8080/service"));
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://100.64.0.1:8080/service"));
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://[::1]:8080/service"));
    Assert.assertNotNull(OutboundUrlValidator.validateOutboundUrl("http://[fd12:3456:789a::1]:8080/service"));

    // Cloud metadata, link-local, unspecified, and cluster-internal names remain unconditionally blocked
    String[] stillBlocked = new String[] {
      "http://169.254.169.254/computeMetadata/v1/",
      "http://169.254.0.1/",
      "http://100.100.100.200/latest/meta-data/",
      "http://0.0.0.0/",
      "http://224.0.0.1/",
      "http://255.255.255.255/",
      "http://[::]/",
      "http://[fe80::1]/",
      "http://[fd00:ec2::254]/",
      "http://[::ffff:169.254.169.254]/",
      "http://metadata.google.internal/computeMetadata/v1/",
      "http://metadata/computeMetadata/v1/",
      "http://kubernetes.default.svc/api",
      "http://cdap-master.default.svc.cluster.local:11015",
      "http://service.internal/api",
      "http://service.local/api"
    };
    for (String url : stillBlocked) {
      assertRejectedUrl(url);
    }
  }

  @Test
  public void testResolveSafeRelativePath() throws Exception {
    URI baseWithoutPath = new URI("https://example.com");
    URI resolved1 = OutboundUrlValidator.resolveSafeRelativePath(baseWithoutPath, "manifest.json");
    Assert.assertEquals("https://example.com/manifest.json", resolved1.toString());

    URI baseWithPath = new URI("https://example.com/datamodels/v1");
    URI resolved2 = OutboundUrlValidator.resolveSafeRelativePath(baseWithPath, "OMOP_6_0_0.avsc");
    Assert.assertEquals("https://example.com/datamodels/v1/OMOP_6_0_0.avsc", resolved2.toString());

    URI resolvedNested = OutboundUrlValidator.resolveSafeRelativePath(baseWithPath, "namespaces/default/schemas/s1");
    Assert.assertEquals("https://example.com/datamodels/v1/namespaces/default/schemas/s1", resolvedNested.toString());

    String[] maliciousRelativePaths = new String[] {
      null,
      "",
      "   ",
      "/169.254.169.254/computeMetadata/v1/",
      "//169.254.169.254/computeMetadata/v1/",
      "http://169.254.169.254/computeMetadata/v1/",
      "../secret.avsc",
      "a/../../secret.avsc",
      "./manifest.json",
      "a//b.avsc",
      "foo\\bar.avsc",
      "manifest.json?url=http://169.254.169.254",
      "manifest.json#fragment",
      "user@169.254.169.254",
      "%2e%2e/secret.avsc",
      "%2f169.254.169.254"
    };
    for (String badPath : maliciousRelativePaths) {
      try {
        OutboundUrlValidator.resolveSafeRelativePath(baseWithoutPath, badPath);
        Assert.fail("Expected relative path to be rejected: " + badPath);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testValidateOutboundHost() throws Exception {
    OutboundUrlValidator.setHostResolverForTesting(host -> new InetAddress[] {
      InetAddress.getByAddress(host, new byte[] {(byte) 40, (byte) 112, (byte) 72, (byte) 205})
    });
    OutboundUrlValidator.validateOutboundHost("myaccount.azuredatalakestore.net");

    String[] invalidHosts = new String[] {
      null,
      "",
      "localhost",
      "127.0.0.1",
      "169.254.169.254",
      "metadata.google.internal",
      "metadata.google.internal\u3002",
      "kubernetes.default.svc.cluster.local",
      "https://myaccount.azuredatalakestore.net",
      "myaccount.azuredatalakestore.net:443",
      "myaccount.azuredatalakestore.net/path"
    };
    for (String host : invalidHosts) {
      try {
        OutboundUrlValidator.validateOutboundHost(host);
        Assert.fail("Expected outbound host to be rejected: " + host);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testCreateSafeHttpClientBlocksDnsRebindingAtConnectTime() throws Exception {
    AtomicInteger lookups = new AtomicInteger(0);
    OutboundUrlValidator.setHostResolverForTesting(host -> {
      if (lookups.incrementAndGet() == 1) {
        return new InetAddress[] {
          InetAddress.getByAddress(host, new byte[] {(byte) 93, (byte) 184, (byte) 216, 34})
        };
      }
      return new InetAddress[] {
        InetAddress.getByAddress(host, new byte[] {(byte) 169, (byte) 254, (byte) 169, (byte) 254})
      };
    });

    URI uri = OutboundUrlValidator.validateOutboundUrl("http://rebind.example.com/resource");
    try (CloseableHttpClient client = OutboundUrlValidator.createSafeHttpClient(1000, 1000)) {
      client.execute(new HttpGet(uri));
      Assert.fail("Expected HttpClient connect-time DNS resolution to reject rebound metadata IP");
    } catch (UnknownHostException expected) {
      Assert.assertTrue(expected.getMessage().contains("Restricted target IP address"));
      Assert.assertTrue(expected.getCause() instanceof IllegalArgumentException);
    }
  }

  private static void assertRejectedUrl(String url) {
    try {
      OutboundUrlValidator.validateOutboundUrl(url);
      Assert.fail("Expected URL to be rejected: " + url);
    } catch (IllegalArgumentException expected) {
      Assert.assertFalse("Rejection should not be due to offline DNS lookup for: " + url,
                         expected.getCause() instanceof UnknownHostException &&
                           !"http://unresolvable.example.com/data".equals(url));
    }
  }
}
