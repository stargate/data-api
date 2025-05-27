package io.stargate.sgv2.jsonapi.api.request;

import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

import io.stargate.sgv2.jsonapi.metrics.TenantRequestMetricsFilter;
import io.stargate.sgv2.jsonapi.metrics.TenantRequestMetricsTagProvider;
import java.util.regex.Pattern;

public class UserAgent {

  // Match on the product name, and then optional version. See usage
  private static final Pattern PRODUCT_VERSION_REGEX =
      Pattern.compile("^([^\\s\\/]+)(?:\\/([^\\s]+))?");

  private final String rawUserAgent;
  private final ProductVersion productVersion;

  public UserAgent(String rawUserAgent) {
    this.rawUserAgent = normalizeOptionalString(rawUserAgent);
    this.productVersion = extractProduct(this.rawUserAgent);
  }

  /**
   * Extract the first product from the user agent.
   *
   * <p>We assume the agent string has the following format:
   *
   * <pre>
   *   User-Agent: product/version (system-information) [additional-details]
   *
   *   e.g. Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1)
   * </pre>
   *
   * In the above example, the product is "Mozilla".
   *
   * <p>This logic used to be in {@link TenantRequestMetricsFilter} and {@link
   * TenantRequestMetricsTagProvider}
   */
  private static ProductVersion extractProduct(String rawUserAgent) {

    var matcher = PRODUCT_VERSION_REGEX.matcher(rawUserAgent);

    if (matcher.find()) {
      return new ProductVersion(matcher.group(1), matcher.group(2));
    }
    // no match, default to using the full user agent as the product name
    return new ProductVersion(
        normalizeOptionalString(rawUserAgent), normalizeOptionalString((String) null));
  }

  public String product() {
    return productVersion.product;
  }

  /**
   * Gets the raw user agent string from the request.
   *
   * @return
   */
  @Override
  public String toString() {
    return rawUserAgent;
  }

  // equals and hash are case-insensitive
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof UserAgent that)) {
      return false;
    }
    return rawUserAgent.equalsIgnoreCase(that.rawUserAgent);
  }

  @Override
  public int hashCode() {
    return rawUserAgent.toLowerCase().hashCode();
  }

  private record ProductVersion(String product, String version) {}
}
