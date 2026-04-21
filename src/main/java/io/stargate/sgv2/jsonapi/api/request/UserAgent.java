package io.stargate.sgv2.jsonapi.api.request;

import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

import io.stargate.sgv2.jsonapi.metrics.TenantRequestMetricsFilter;
import io.stargate.sgv2.jsonapi.metrics.TenantRequestMetricsTagProvider;
import java.util.regex.Pattern;

/**
 * The User-Agent making the call to the API, we always have a User-Agent even if the header was
 * missing or empty.
 *
 * <p>This class extracts the product name and version from the User-Agent string, which is used for
 * metrics and logging purposes.
 *
 * <p>The raw user agent string is normalized to be an empty string, and can be obtained using
 * {@link #toString()}. NOTE: you should normally compare UserAgent instances, not the raw string.
 *
 * <p>It is used in {@link TenantRequestMetricsFilter} and {@link TenantRequestMetricsTagProvider}
 *
 * <p>See examples in the unit tests.
 */
public class UserAgent {

  // Match on the product name, and then optional version. See {@link #extractProduct(String)}
  private static final Pattern PRODUCT_VERSION_REGEX =
      Pattern.compile("^([^\\s\\/]+)(?:\\/([^\\s]+))?");

  private final String rawUserAgent;
  private final ProductVersion productVersion;

  /**
   * Constructs a UserAgent instance from the raw user agent string.
   *
   * @param rawUserAgent nullable or empty string is normalized with {@link
   *     io.stargate.sgv2.jsonapi.util.StringUtil#normalizeOptionalString(String)} to be non-null.
   */
  public UserAgent(String rawUserAgent) {
    this.rawUserAgent = normalizeOptionalString(rawUserAgent);
    this.productVersion = extractProduct(this.rawUserAgent);
  }

  /** Gets the left most product name from the user agent. */
  public String product() {
    return productVersion.product;
  }

  /** Gets the raw user agent string from the request. */
  @Override
  public String toString() {
    return rawUserAgent;
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

  /**
   * Compares this UserAgent with another object for equality, using the full raw string as
   * comparing case-insensitive.
   */
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
