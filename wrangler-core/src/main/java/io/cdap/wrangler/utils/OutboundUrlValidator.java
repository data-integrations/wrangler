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

import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.net.IDN;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Validates outbound HTTP/HTTPS URLs, hostnames, and resolved IP addresses to prevent
 * Server-Side Request Forgery (SSRF) against cloud metadata endpoints, loopback interfaces,
 * cluster-internal services, and private networks.
 */
public final class OutboundUrlValidator {

  /**
   * System property that, when set to {@code "true"}, permits outbound connections to loopback
   * and RFC 1918 / RFC 4193 / RFC 6598 private networks while keeping cloud metadata, link-local,
   * unspecified, multicast, broadcast, and cluster-internal hostnames unconditionally blocked.
   */
  public static final String ALLOW_PRIVATE_NETWORKS_PROPERTY =
    "wrangler.outbound.http.allow.private.networks";

  public static final int DEFAULT_CONNECT_TIMEOUT_MS = 10000;
  public static final int DEFAULT_READ_TIMEOUT_MS = 30000;

  private static final Set<String> ALLOWED_SCHEMES = Collections.unmodifiableSet(
    new HashSet<>(Arrays.asList("http", "https")));

  private static final Set<String> ALWAYS_BLOCKED_HOSTS = Collections.unmodifiableSet(
    new HashSet<>(Arrays.asList(
      "metadata",
      "metadata.google.internal",
      "metadata.goog",
      "kubernetes",
      "kubernetes.default",
      "kubernetes.default.svc",
      "broadcasthost"
    )));

  private static final Set<String> LOOPBACK_HOSTS = Collections.unmodifiableSet(
    new HashSet<>(Arrays.asList(
      "localhost",
      "ip6-localhost",
      "ip6-loopback"
    )));

  private static final Set<String> ALWAYS_BLOCKED_HOST_SUFFIXES = Collections.unmodifiableSet(
    new HashSet<>(Arrays.asList(
      ".internal",
      ".local",
      ".cluster.local",
      ".svc",
      ".pod",
      ".metadata.google.internal",
      ".google.internal"
    )));

  /**
   * Pluggable DNS resolver hook for deterministic unit testing.
   */
  interface HostResolver {
    InetAddress[] resolve(String host) throws UnknownHostException;
  }

  private static volatile HostResolver hostResolver = InetAddress::getAllByName;

  private OutboundUrlValidator() {
    // Utility class
  }

  static void setHostResolverForTesting(HostResolver resolver) {
    hostResolver = Objects.requireNonNull(resolver, "resolver");
  }

  static void resetHostResolverForTesting() {
    hostResolver = InetAddress::getAllByName;
  }

  /**
   * Returns true if private/loopback network access is explicitly enabled via system property.
   */
  public static boolean isPrivateNetworksAllowed() {
    return Boolean.parseBoolean(System.getProperty(ALLOW_PRIVATE_NETWORKS_PROPERTY, "false"));
  }

  /**
   * Validates the URL syntax, scheme ({@code http} or {@code https}), authority, and hostname
   * (including literal IP range validation) without performing DNS resolution for domain names.
   *
   * @param urlString the outbound URL string to validate
   * @return the validated {@link URI}
   * @throws IllegalArgumentException if the URL is malformed or targets a prohibited scheme, host, or IP
   */
  public static URI validateSyntaxAndHost(String urlString) {
    if (urlString == null) {
      throw new IllegalArgumentException("Outbound URL must not be null.");
    }
    String trimmed = urlString.trim();
    if (trimmed.isEmpty() || containsControlChars(trimmed)) {
      throw new IllegalArgumentException("Outbound URL is empty or contains invalid control characters.");
    }

    URI uri;
    URL url;
    try {
      uri = new URI(trimmed);
      url = new URL(trimmed);
    } catch (URISyntaxException | MalformedURLException e) {
      throw new IllegalArgumentException("Invalid outbound URL syntax: " + urlString, e);
    }

    String scheme = uri.getScheme();
    String urlProtocol = url.getProtocol();
    if (scheme == null || urlProtocol == null || !scheme.equalsIgnoreCase(urlProtocol)) {
      throw new IllegalArgumentException("Outbound URL must specify a valid scheme.");
    }
    String normalizedScheme = scheme.toLowerCase(Locale.ROOT);
    if (!ALLOWED_SCHEMES.contains(normalizedScheme)) {
      throw new IllegalArgumentException(
        String.format("Unsupported outbound URL scheme '%s'. Only http and https are allowed.", scheme));
    }

    if (uri.getRawUserInfo() != null || uri.getUserInfo() != null || url.getUserInfo() != null) {
      throw new IllegalArgumentException("Outbound URL must not contain user info credentials.");
    }

    String rawAuthority = uri.getRawAuthority();
    if (rawAuthority == null || rawAuthority.isEmpty()
        || rawAuthority.contains("@") || rawAuthority.contains("\\")) {
      throw new IllegalArgumentException("Outbound URL contains an invalid authority component.");
    }

    String host = uri.getHost();
    String urlHost = url.getHost();
    if (host == null || host.isEmpty() || urlHost == null || !host.equalsIgnoreCase(urlHost)) {
      throw new IllegalArgumentException("Outbound URL must specify a valid host.");
    }

    int port = uri.getPort();
    if (port < -1 || port == 0 || port > 65535) {
      throw new IllegalArgumentException("Outbound URL specifies an invalid port: " + port);
    }

    validateHostName(host, isPrivateNetworksAllowed());
    return uri;
  }

  /**
   * Validates the outbound URL syntax, scheme, hostname, and resolves all DNS A/AAAA records
   * to verify that every resolved IP address is permitted.
   *
   * @param urlString the outbound URL string to validate
   * @return the validated {@link URI}
   * @throws IllegalArgumentException if the URL or any of its resolved IP addresses is prohibited
   */
  public static URI validateOutboundUrl(String urlString) {
    URI uri = validateSyntaxAndHost(urlString);
    try {
      resolveAndValidate(uri.getHost());
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Unable to resolve host for outbound URL: " + uri.getHost(), e);
    }
    return uri;
  }

  /**
   * Validates a standalone outbound hostname (for example, an ADLS account FQDN) and verifies
   * all of its resolved DNS A/AAAA records.
   *
   * @param host the hostname to validate
   * @throws IllegalArgumentException if the hostname or any of its resolved IP addresses is prohibited
   */
  public static void validateOutboundHost(String host) {
    if (host == null) {
      throw new IllegalArgumentException("Outbound host must not be null.");
    }
    String trimmed = host.trim();
    if (trimmed.isEmpty() || containsControlChars(trimmed)
        || trimmed.contains("/") || trimmed.contains("\\")
        || trimmed.contains("@") || trimmed.contains("?")
        || trimmed.contains("#") || trimmed.contains(":")) {
      throw new IllegalArgumentException("Invalid outbound host: " + host);
    }
    try {
      resolveAndValidate(trimmed);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Unable to resolve outbound host: " + trimmed, e);
    }
  }

  /**
   * Validates the hostname and resolves all DNS A/AAAA records, rejecting the host if any
   * resolved address falls within a prohibited IP range.
   *
   * @param host the hostname or IP literal to resolve and validate
   * @return the validated non-empty array of {@link InetAddress} instances
   * @throws UnknownHostException if the host cannot be resolved
   * @throws IllegalArgumentException if the hostname or any resolved address is prohibited
   */
  public static InetAddress[] resolveAndValidate(String host) throws UnknownHostException {
    boolean allowPrivate = isPrivateNetworksAllowed();
    validateHostName(host, allowPrivate);
    InetAddress[] addresses = hostResolver.resolve(host);
    if (addresses == null || addresses.length == 0) {
      throw new UnknownHostException("No addresses resolved for host: " + host);
    }
    for (InetAddress address : addresses) {
      if (address == null) {
        throw new IllegalArgumentException("Resolved null address for host: " + host);
      }
      validateAddress(host, address, allowPrivate);
    }
    return addresses;
  }

  /**
   * Resolves a relative child path against a base URI while guaranteeing that the target scheme,
   * authority, host, and port remain unchanged and that the normalized path stays beneath the
   * base URI's path prefix.
   *
   * @param baseUri the validated base URI
   * @param relativePath the relative path to append
   * @return the resolved child {@link URI}
   * @throws IllegalArgumentException if {@code relativePath} attempts path traversal or authority override
   */
  public static URI resolveSafeRelativePath(URI baseUri, String relativePath) {
    if (baseUri == null) {
      throw new IllegalArgumentException("Base URI must not be null.");
    }
    if (relativePath == null) {
      throw new IllegalArgumentException("Relative path must not be null.");
    }
    String trimmed = relativePath.trim();
    if (trimmed.isEmpty() || containsControlChars(trimmed)) {
      throw new IllegalArgumentException("Relative path must not be empty or contain control characters.");
    }
    String lower = trimmed.toLowerCase(Locale.ROOT);
    if (trimmed.startsWith("/") || trimmed.contains("\\") || trimmed.contains("://")
        || trimmed.contains("?") || trimmed.contains("#") || trimmed.contains("@")
        || lower.contains("%2e") || lower.contains("%2f") || lower.contains("%5c")) {
      throw new IllegalArgumentException("Invalid relative resource path: " + relativePath);
    }

    String[] segments = trimmed.split("/", -1);
    for (String segment : segments) {
      if (segment.isEmpty() || ".".equals(segment) || "..".equals(segment)) {
        throw new IllegalArgumentException("Relative resource path must not contain traversal segments: "
                                           + relativePath);
      }
    }

    String basePath = baseUri.getPath() == null ? "" : baseUri.getPath();
    String normalizedBasePrefix = basePath.endsWith("/") ? basePath : basePath + "/";
    String combinedPath = normalizedBasePrefix + trimmed;

    URI resolved;
    try {
      resolved = new URI(
        baseUri.getScheme(),
        baseUri.getUserInfo(),
        baseUri.getHost(),
        baseUri.getPort(),
        combinedPath,
        baseUri.getQuery(),
        baseUri.getFragment()
      ).normalize();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to construct resource URI for path: " + relativePath, e);
    }

    if (!Objects.equals(baseUri.getScheme(), resolved.getScheme())
        || !Objects.equals(baseUri.getAuthority(), resolved.getAuthority())
        || !Objects.equals(baseUri.getHost(), resolved.getHost())
        || baseUri.getPort() != resolved.getPort()
        || resolved.getPath() == null
        || !resolved.getPath().startsWith(normalizedBasePrefix)) {
      throw new IllegalArgumentException("Resolved path escapes base URI authority or path prefix: " + relativePath);
    }
    return resolved;
  }

  /**
   * Creates an Apache {@link CloseableHttpClient} with automatic redirect following disabled
   * and DNS resolution pinned through {@link #resolveAndValidate(String)} to prevent
   * DNS-rebinding TOCTOU and HTTP 30x redirect SSRF bypasses.
   *
   * @param connectTimeoutMs connection establishment timeout in milliseconds
   * @param readTimeoutMs socket read timeout in milliseconds
   * @return a configured {@link CloseableHttpClient}
   */
  public static CloseableHttpClient createSafeHttpClient(int connectTimeoutMs, int readTimeoutMs) {
    int effectiveConnectTimeout = connectTimeoutMs > 0 ? connectTimeoutMs : DEFAULT_CONNECT_TIMEOUT_MS;
    int effectiveReadTimeout = readTimeoutMs > 0 ? readTimeoutMs : DEFAULT_READ_TIMEOUT_MS;
    RequestConfig requestConfig = RequestConfig.custom()
      .setConnectTimeout(effectiveConnectTimeout)
      .setConnectionRequestTimeout(effectiveConnectTimeout)
      .setSocketTimeout(effectiveReadTimeout)
      .setRedirectsEnabled(false)
      .build();

    DnsResolver pinnedDnsResolver = host -> {
      try {
        return resolveAndValidate(host);
      } catch (IllegalArgumentException e) {
        UnknownHostException uhe = new UnknownHostException("Restricted target IP address: " + e.getMessage());
        uhe.initCause(e);
        throw uhe;
      }
    };
    return HttpClients.custom()
      .setDefaultRequestConfig(requestConfig)
      .disableRedirectHandling()
      .setDnsResolver(pinnedDnsResolver)
      .build();
  }

  static void validateHostName(String rawHost, boolean allowPrivateNetworks) {
    if (rawHost == null) {
      throw new IllegalArgumentException("Host must not be null.");
    }
    String trimmed = rawHost.trim();
    if (trimmed.isEmpty() || containsControlChars(trimmed)) {
      throw new IllegalArgumentException("Host must not be empty.");
    }
    if (trimmed.contains("%")) {
      throw new IllegalArgumentException("IPv6 zone indices are not allowed in outbound host: " + rawHost);
    }

    if (trimmed.startsWith("[") || trimmed.endsWith("]")) {
      if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
        throw new IllegalArgumentException("Malformed IPv6 literal host: " + rawHost);
      }
      String inner = trimmed.substring(1, trimmed.length() - 1);
      if (inner.isEmpty() || inner.contains("[") || inner.contains("]") || !inner.contains(":")) {
        throw new IllegalArgumentException("Malformed IPv6 literal host: " + rawHost);
      }
      InetAddress addr = parseIpLiteralWithoutDns(trimmed, rawHost);
      validateAddress(rawHost, addr, allowPrivateNetworks);
      return;
    }

    if (trimmed.contains(":")) {
      InetAddress addr = parseIpLiteralWithoutDns("[" + trimmed + "]", rawHost);
      validateAddress(rawHost, addr, allowPrivateNetworks);
      return;
    }

    String hostWithoutTrailingDots = trimmed;
    while (hostWithoutTrailingDots.endsWith(".")) {
      hostWithoutTrailingDots = hostWithoutTrailingDots.substring(0, hostWithoutTrailingDots.length() - 1);
    }
    if (hostWithoutTrailingDots.isEmpty() || hostWithoutTrailingDots.startsWith(".")) {
      throw new IllegalArgumentException("Invalid host in outbound URL: " + rawHost);
    }

    String asciiHost;
    try {
      asciiHost = IDN.toASCII(hostWithoutTrailingDots, IDN.ALLOW_UNASSIGNED).toLowerCase(Locale.ROOT);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid host in outbound URL: " + rawHost, e);
    }
    while (asciiHost.endsWith(".")) {
      asciiHost = asciiHost.substring(0, asciiHost.length() - 1);
    }

    if (asciiHost.isEmpty() || asciiHost.startsWith(".") || asciiHost.contains("..")
        || containsProhibitedHostChars(asciiHost)) {
      throw new IllegalArgumentException("Invalid host in outbound URL: " + rawHost);
    }

    if (ALWAYS_BLOCKED_HOSTS.contains(asciiHost) || asciiHost.startsWith("kubernetes.default.")) {
      throw new IllegalArgumentException("Outbound connection to internal host is blocked: " + rawHost);
    }
    for (String suffix : ALWAYS_BLOCKED_HOST_SUFFIXES) {
      if (asciiHost.endsWith(suffix)) {
        throw new IllegalArgumentException("Outbound connection to internal host suffix is blocked: " + rawHost);
      }
    }

    if (!allowPrivateNetworks) {
      if (LOOPBACK_HOSTS.contains(asciiHost) || asciiHost.endsWith(".localhost")) {
        throw new IllegalArgumentException("Outbound connection to loopback host is blocked: " + rawHost);
      }
    }

    String[] labels = asciiHost.split("\\.", -1);
    String lastLabel = labels[labels.length - 1];
    if (isNumericOrHexLabel(lastLabel)) {
      byte[] ipv4Bytes = parseStrictCanonicalIpv4(labels, rawHost);
      try {
        InetAddress addr = InetAddress.getByAddress(asciiHost, ipv4Bytes);
        validateAddress(rawHost, addr, allowPrivateNetworks);
      } catch (UnknownHostException e) {
        throw new IllegalArgumentException("Invalid IPv4 literal host: " + rawHost, e);
      }
    }
  }

  /**
   * Validates a single resolved {@link InetAddress} against prohibited IPv4 and IPv6 ranges.
   */
  public static void validateAddress(String host, InetAddress address, boolean allowPrivateNetworks) {
    if (address == null) {
      throw new IllegalArgumentException("Address must not be null.");
    }
    if (address instanceof Inet4Address) {
      validateIpv4Bytes(host, address.getAddress(), allowPrivateNetworks);
    } else if (address instanceof Inet6Address) {
      validateIpv6Address(host, (Inet6Address) address, allowPrivateNetworks);
    } else {
      throw new IllegalArgumentException("Unsupported InetAddress type for host: " + host);
    }
  }

  private static void validateIpv4Bytes(String host, byte[] bytes, boolean allowPrivateNetworks) {
    if (bytes == null || bytes.length != 4) {
      throw new IllegalArgumentException("Invalid IPv4 address bytes for host: " + host);
    }
    int b0 = bytes[0] & 0xFF;
    int b1 = bytes[1] & 0xFF;
    int b2 = bytes[2] & 0xFF;
    int b3 = bytes[3] & 0xFF;

    // 0.0.0.0/8 (unspecified / "this" network)
    if (b0 == 0) {
      throw new IllegalArgumentException("Outbound connection to unspecified address 0.0.0.0/8 is blocked: " + host);
    }
    // 169.254.0.0/16 (link-local & cloud instance metadata 169.254.169.254)
    if (b0 == 169 && b1 == 254) {
      throw new IllegalArgumentException(
        "Outbound connection to link-local or metadata address 169.254.0.0/16 is blocked: " + host);
    }
    // 100.100.100.200/32 (cloud metadata endpoint)
    if (b0 == 100 && b1 == 100 && b2 == 100 && b3 == 200) {
      throw new IllegalArgumentException("Outbound connection to metadata address is blocked: " + host);
    }
    // 224.0.0.0/4 (multicast) and 240.0.0.0/4 (reserved / broadcast 255.255.255.255)
    if (b0 >= 224) {
      throw new IllegalArgumentException(
        "Outbound connection to multicast or reserved address is blocked: " + host);
    }
    // 192.0.0.0/24 (IETF protocol assignments) and 192.0.2.0/24 (TEST-NET-1)
    if (b0 == 192 && b1 == 0 && (b2 == 0 || b2 == 2)) {
      throw new IllegalArgumentException("Outbound connection to reserved IETF/TEST-NET range is blocked: " + host);
    }
    // 198.18.0.0/15 (benchmarking)
    if (b0 == 198 && (b1 == 18 || b1 == 19)) {
      throw new IllegalArgumentException("Outbound connection to benchmarking range 198.18.0.0/15 is blocked: " + host);
    }
    // 198.51.100.0/24 (TEST-NET-2)
    if (b0 == 198 && b1 == 51 && b2 == 100) {
      throw new IllegalArgumentException("Outbound connection to TEST-NET-2 range is blocked: " + host);
    }
    // 203.0.113.0/24 (TEST-NET-3)
    if (b0 == 203 && b1 == 0 && b2 == 113) {
      throw new IllegalArgumentException("Outbound connection to TEST-NET-3 range is blocked: " + host);
    }

    if (!allowPrivateNetworks) {
      // 127.0.0.0/8 (loopback)
      if (b0 == 127) {
        throw new IllegalArgumentException("Outbound connection to loopback address 127.0.0.0/8 is blocked: " + host);
      }
      // 10.0.0.0/8 (RFC 1918)
      if (b0 == 10) {
        throw new IllegalArgumentException("Outbound connection to private address 10.0.0.0/8 is blocked: " + host);
      }
      // 172.16.0.0/12 (RFC 1918)
      if (b0 == 172 && b1 >= 16 && b1 <= 31) {
        throw new IllegalArgumentException("Outbound connection to private address 172.16.0.0/12 is blocked: " + host);
      }
      // 192.168.0.0/16 (RFC 1918)
      if (b0 == 192 && b1 == 168) {
        throw new IllegalArgumentException("Outbound connection to private address 192.168.0.0/16 is blocked: " + host);
      }
      // 100.64.0.0/10 (Carrier-Grade NAT RFC 6598)
      if (b0 == 100 && b1 >= 64 && b1 <= 127) {
        throw new IllegalArgumentException("Outbound connection to CGNAT address 100.64.0.0/10 is blocked: " + host);
      }
    }
  }

  private static void validateIpv6Address(String host, Inet6Address addr, boolean allowPrivateNetworks) {
    if (addr.getScopeId() > 0 || addr.getScopedInterface() != null) {
      throw new IllegalArgumentException("IPv6 scoped address is blocked: " + host);
    }
    byte[] b = addr.getAddress();
    if (b == null || b.length != 16) {
      throw new IllegalArgumentException("Invalid IPv6 address bytes for host: " + host);
    }

    int b0 = b[0] & 0xFF;
    int b1 = b[1] & 0xFF;

    // Unspecified ::
    if (addr.isAnyLocalAddress()) {
      throw new IllegalArgumentException("Outbound connection to unspecified IPv6 address :: is blocked: " + host);
    }
    // Link-local fe80::/10
    if (addr.isLinkLocalAddress() || (b0 == 0xFE && (b1 & 0xC0) == 0x80)) {
      throw new IllegalArgumentException("Outbound connection to link-local IPv6 address fe80::/10 is blocked: "
                                         + host);
    }
    // Multicast ff00::/8
    if (addr.isMulticastAddress() || b0 == 0xFF) {
      throw new IllegalArgumentException("Outbound connection to multicast IPv6 address ff00::/8 is blocked: " + host);
    }
    // Cloud metadata IPv6 fd00:ec2::254
    if (isEc2Ipv6Metadata(b)) {
      throw new IllegalArgumentException("Outbound connection to IPv6 cloud metadata address is blocked: " + host);
    }
    // Documentation 2001:db8::/32
    if (b0 == 0x20 && b1 == 0x01 && (b[2] & 0xFF) == 0x0D && (b[3] & 0xFF) == 0xB8) {
      throw new IllegalArgumentException("Outbound connection to documentation IPv6 range 2001:db8::/32 is blocked: "
                                         + host);
    }
    // Discard-only 100::/64
    if (b0 == 0x01 && b1 == 0x00 && isZeroRange(b, 2, 8)) {
      throw new IllegalArgumentException("Outbound connection to discard-only IPv6 range 100::/64 is blocked: " + host);
    }

    if (!allowPrivateNetworks) {
      // Loopback ::1
      if (addr.isLoopbackAddress()) {
        throw new IllegalArgumentException("Outbound connection to IPv6 loopback address ::1 is blocked: " + host);
      }
      // Site-local fec0::/10
      if (addr.isSiteLocalAddress() || (b0 == 0xFE && (b1 & 0xC0) == 0xC0)) {
        throw new IllegalArgumentException("Outbound connection to site-local IPv6 address is blocked: " + host);
      }
      // Unique Local Address fc00::/7
      if ((b0 & 0xFE) == 0xFC) {
        throw new IllegalArgumentException("Outbound connection to unique-local IPv6 range fc00::/7 is blocked: "
                                           + host);
      }
    }

    // Check embedded IPv4 addresses in IPv6 transition mechanisms:
    // 1. IPv4-mapped (::ffff:w.x.y.z)
    if (isZeroRange(b, 0, 10) && (b[10] & 0xFF) == 0xFF && (b[11] & 0xFF) == 0xFF) {
      validateIpv4Bytes(host, Arrays.copyOfRange(b, 12, 16), allowPrivateNetworks);
    }
    // 2. IPv4-compatible (::w.x.y.z, excluding :: and ::1)
    if (isZeroRange(b, 0, 12) && !addr.isAnyLocalAddress() && !addr.isLoopbackAddress()) {
      validateIpv4Bytes(host, Arrays.copyOfRange(b, 12, 16), allowPrivateNetworks);
    }
    // 3. SIIT IPv4-translated (::ffff:0:w.x.y.z)
    if (isZeroRange(b, 0, 8) && (b[8] & 0xFF) == 0xFF && (b[9] & 0xFF) == 0xFF
        && b[10] == 0 && b[11] == 0) {
      validateIpv4Bytes(host, Arrays.copyOfRange(b, 12, 16), allowPrivateNetworks);
    }
    // 4. NAT64 well-known prefix 64:ff9b::/96 and local-use 64:ff9b:1::/48
    if (b0 == 0x00 && b1 == 0x64 && (b[2] & 0xFF) == 0xFF && (b[3] & 0xFF) == 0x9B) {
      if (isZeroRange(b, 4, 12) || (b[4] == 0x00 && b[5] == 0x01)) {
        validateIpv4Bytes(host, Arrays.copyOfRange(b, 12, 16), allowPrivateNetworks);
      }
    }
    // 5. 6to4 (2002::/16 -> embedded IPv4 in bytes 2..5)
    if (b0 == 0x20 && b1 == 0x02) {
      validateIpv4Bytes(host, Arrays.copyOfRange(b, 2, 6), allowPrivateNetworks);
    }
    // 6. Teredo (2001:0000::/32 -> server IPv4 in bytes 4..7, obfuscated client IPv4 in bytes 12..15)
    if (b0 == 0x20 && b1 == 0x01 && b[2] == 0x00 && b[3] == 0x00) {
      byte[] clientIpv4 = new byte[] {
        (byte) (b[12] ^ 0xFF),
        (byte) (b[13] ^ 0xFF),
        (byte) (b[14] ^ 0xFF),
        (byte) (b[15] ^ 0xFF)
      };
      validateIpv4Bytes(host, clientIpv4, allowPrivateNetworks);
      validateIpv4Bytes(host, Arrays.copyOfRange(b, 4, 8), allowPrivateNetworks);
    }
    // 7. ISATAP (interface identifier 0000:5efe:w.x.y.z or 0200:5efe:w.x.y.z)
    if (((b[8] & 0xFD) == 0x00) && b[9] == 0x00 && (b[10] & 0xFF) == 0x5E && (b[11] & 0xFF) == 0xFE) {
      validateIpv4Bytes(host, Arrays.copyOfRange(b, 12, 16), allowPrivateNetworks);
    }
  }

  private static boolean isEc2Ipv6Metadata(byte[] b) {
    return (b[0] & 0xFF) == 0xFD && b[1] == 0x00
      && (b[2] & 0xFF) == 0x0E && (b[3] & 0xFF) == 0xC2
      && isZeroRange(b, 4, 14)
      && (b[14] & 0xFF) == 0x02 && (b[15] & 0xFF) == 0x54;
  }

  private static boolean isZeroRange(byte[] b, int startInclusive, int endExclusive) {
    for (int i = startInclusive; i < endExclusive; i++) {
      if (b[i] != 0) {
        return false;
      }
    }
    return true;
  }

  private static InetAddress parseIpLiteralWithoutDns(String bracketedIpv6, String rawHost) {
    try {
      return InetAddress.getByName(bracketedIpv6);
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException("Invalid IP literal host: " + rawHost, e);
    }
  }

  private static boolean isNumericOrHexLabel(String label) {
    if (label == null || label.isEmpty()) {
      return false;
    }
    if (label.startsWith("0x") || label.startsWith("0X")) {
      if (label.length() <= 2) {
        return false;
      }
      for (int i = 2; i < label.length(); i++) {
        char c = label.charAt(i);
        boolean isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
        if (!isHex) {
          return false;
        }
      }
      return true;
    }
    for (int i = 0; i < label.length(); i++) {
      char c = label.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  private static byte[] parseStrictCanonicalIpv4(String[] labels, String rawHost) {
    if (labels.length != 4) {
      throw new IllegalArgumentException(
        "Non-standard or shorthand numeric IPv4 encoding is not allowed: " + rawHost);
    }
    byte[] result = new byte[4];
    for (int i = 0; i < 4; i++) {
      String part = labels[i];
      if (part.isEmpty() || part.length() > 3) {
        throw new IllegalArgumentException("Invalid IPv4 octet in host: " + rawHost);
      }
      if (part.length() > 1 && part.charAt(0) == '0') {
        throw new IllegalArgumentException("Octal or leading-zero IPv4 encoding is not allowed: " + rawHost);
      }
      for (int j = 0; j < part.length(); j++) {
        char c = part.charAt(j);
        if (c < '0' || c > '9') {
          throw new IllegalArgumentException("Non-decimal IPv4 octet is not allowed: " + rawHost);
        }
      }
      int value = Integer.parseInt(part);
      if (value < 0 || value > 255) {
        throw new IllegalArgumentException("IPv4 octet out of range in host: " + rawHost);
      }
      result[i] = (byte) value;
    }
    return result;
  }

  private static boolean containsControlChars(String value) {
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c <= 0x1F || c == 0x7F) {
        return true;
      }
    }
    return false;
  }

  private static boolean containsProhibitedHostChars(String host) {
    for (int i = 0; i < host.length(); i++) {
      char c = host.charAt(i);
      if (c <= 0x20 || c == 0x7F || c == '/' || c == '\\' || c == '@' || c == '?' || c == '#' || c == ':') {
        return true;
      }
    }
    return false;
  }
}
