package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.Map;

/** Extra, Stargate related configuration for the metrics. */
@ConfigMapping(prefix = "stargate.metrics")
public interface MetricsConfig {

  /**
   * @return Global tags attached to each metric being recorded.
   */
  Map<String, String> globalTags();

  /**
   * @return Setup for the tenant request counting.
   */
  @NotNull
  @Valid
  TenantRequestCounterConfig tenantRequestCounter();

  interface TenantRequestCounterConfig {

    /**
     * @return If tenant request counter is enabled.
     */
    @WithDefault("${stargate.multi-tenancy.enabled}")
    boolean enabled();

    /**
     * @return The metric name for the counter, defaults to <code>http.server.requests.counter
     *     </code>.
     */
    @NotBlank
    @WithDefault("http.server.requests.counter")
    String metricName();

    /**
     * @return The tag key for tenant-id, defaults to <code>tenant</code>.
     */
    @NotBlank
    @WithDefault("tenant")
    String tenantTag();

    /**
     * @return The tag key for user-agent flag, defaults to <code>user_agent</code>.
     */
    @NotBlank
    @WithDefault("user_agent")
    String userAgentTag();

    /**
     * @return If tenant counting metric should include the user agent information.
     */
    @WithDefault("false")
    boolean userAgentTagEnabled();

    /**
     * @return The tag key for HTTP status flag, defaults to <code>status</code>.
     */
    @NotBlank
    @WithDefault("status")
    String statusTag();

    /**
     * @return If tenant counting metric should include the HTTP status information.
     */
    @WithDefault("false")
    boolean statusTagEnabled();
  }
}
