package io.stargate.sgv2.jsonapi.api.v1.metrics;

import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics.HTTP_SERVER_REQUESTS;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics.RERANK_ALL_CALL_DURATION_METRIC;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics.RERANK_TENANT_CALL_DURATION_METRIC;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics.VECTORIZE_CALL_DURATION_METRIC;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.smallrye.config.SmallRyeConfig;
import io.stargate.sgv2.jsonapi.config.constants.MetricsConstants;
import jakarta.enterprise.inject.Produces;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Centralized configuration of Micrometer {@link MeterFilter}s.
 *
 * <p>This class provides CDI producer methods for {@link MeterFilter} beans that:
 *
 * <ul>
 *   <li>Apply global tags (e.g., {@code module=sgv2-jsonapi}) to all metrics based on configuration
 *       provided by {@link MetricsConfig}.
 *   <li>Configure distribution statistics (client-side percentiles, Prometheus histogram buckets)
 *       for specific timer metrics using constants defined in {@link
 *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants}.
 * </ul>
 */
public final class MicrometerConfiguration {

  // Private constructor to prevent instantiation
  private MicrometerConfiguration() {}

  /**
   * Produces a meter filter that applies configured global tags (e.g., {@code module=sgv2-jsonapi})
   * to all metrics. Reads tags from {@link MetricsConfig#globalTags()}.
   *
   * @return A {@link MeterFilter} for applying common tags, or an no-op filter if no global tags
   *     are configured.
   */
  @Produces
  public MeterFilter globalTagsMeterFilter() {
    MetricsConfig metricsConfig =
        ConfigProvider.getConfig()
            .unwrap(SmallRyeConfig.class)
            .getConfigMapping(MetricsConfig.class);

    Map<String, String> globalTags = metricsConfig.globalTags();

    // if we have no global tags, use empty (no-op filter)
    if (null == globalTags || globalTags.isEmpty()) {
      return new MeterFilter() {};
    }

    // transform to tags
    Collection<Tag> tags =
        globalTags.entrySet().stream()
            .map(e -> Tag.of(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

    // Notes from PR 2003: This producer method globalTagsMeterFilter is typically called only once
    // during the application startup phase by the Micrometer extension when it's collecting all the
    // MeterFilter beans. It's not called repeatedly during the application's runtime for every
    // metric being recorded. So no need the cache.
    return MeterFilter.commonTags(Tags.of(tags));
  }

  /**
   * Produces a meter filter to configure distribution statistics for key timer metrics such as HTTP
   * server requests, vectorization duration, and reranking calls.
   *
   * <p>This filter enables Prometheus-compatible histogram buckets (allowing server-side {@code
   * histogram_quantile} calculations) and configures specific client-side percentiles.
   *
   * <p>The percentile configuration strategy aims to balance detail with monitoring system load,
   * particularly concerning metric cardinality:
   *
   * <ul>
   *   <li>Metrics with high-cardinality tags (e.g., including {@code tenant}) like {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics#RERANK_TENANT_CALL_DURATION_METRIC}
   *       are configured with a limited set of essential percentiles (e.g., P98) to minimize
   *       overhead.
   *   <li>Metrics aggregated across tenants or with lower cardinality tags, such as {@value
   *       MetricsConstants.Metrics#HTTP_SERVER_REQUESTS} and {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.Metrics#RERANK_ALL_CALL_DURATION_METRIC},
   *       receive a more comprehensive set of standard percentiles (e.g., P50, P90, P95, P99).
   * </ul>
   *
   * Note: The percentiles Pxx correspond to the quantiles 0.xx used in configuration.
   *
   * @return A {@link MeterFilter} for configuring distribution statistics.
   */
  @Produces
  public MeterFilter configureDistributionStatistics() {
    // --- Define Percentile Values ---
    final double[] allTenantLatencyPercentiles = {0.5, 0.90, 0.95, 0.99};
    final double[] perTenantLatencyPercentiles = {0.98};

    // --- Create A Map For Value Look Up ---
    final Map<String, double[]> percentileConfigs =
        Map.of(
            HTTP_SERVER_REQUESTS,
            allTenantLatencyPercentiles,
            VECTORIZE_CALL_DURATION_METRIC,
            allTenantLatencyPercentiles,
            RERANK_TENANT_CALL_DURATION_METRIC,
            perTenantLatencyPercentiles,
            RERANK_ALL_CALL_DURATION_METRIC,
            allTenantLatencyPercentiles);

    return new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(
          Meter.Id id, DistributionStatisticConfig config) {

        // Look up using the above map to find the specific percentiles for the metric
        double[] specificPercentiles = percentileConfigs.get(id.getName());

        // If a specific configuration was found for this metric name
        if (specificPercentiles != null) {
          DistributionStatisticConfig.Builder builder =
              DistributionStatisticConfig.builder().percentiles(specificPercentiles);

          // Enable Prometheus histogram buckets
          builder.percentilesHistogram(true);

          // Merge the specific config with the existing/default config
          return builder.build().merge(config);
        }

        // For all other metrics, return the default configuration
        return config;
      }
    };
  }
}
