package io.stargate.sgv2.jsonapi.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.smallrye.config.SmallRyeConfig;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.SESSION_TAG;
import static io.stargate.sgv2.jsonapi.metrics.MetricsConstants.MetricTags.TENANT_TAG;

/**
 * Centralized configuration of Micrometer {@link MeterFilter}s.
 *
 * <p>This class provides CDI producer methods for {@link MeterFilter} beans that:
 *
 * <ul>
 *   <li>Apply global tags (e.g., {@code module=sgv2-jsonapi}) to all metrics based on configuration
 *       provided by {@link MetricsConfig}, see {@link #globalTagsMeterFilter()}.</li>
 *   <li>Configure distribution statistics percentiles for timer metrics such as HTTP server
 *   see {@link #configureDistributionStatistics()}.</li>
 * </ul>
 */
public class MicrometerConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerConfiguration.class);

  /**
   * Produces a meter filter that applies configured global tags (e.g., {@code module=sgv2-jsonapi})
   * to all metrics. Reads tags from {@link MetricsConfig#globalTags()}.
   *
   * @return A {@link MeterFilter} for applying common tags, or an no-op filter if no global tags
   *     are configured.
   */
  @Produces
  @SuppressWarnings("unused")
  public MeterFilter globalTagsMeterFilter() {
    MetricsConfig metricsConfig =
        ConfigProvider.getConfig()
            .unwrap(SmallRyeConfig.class)
            .getConfigMapping(MetricsConfig.class);

    Map<String, String> globalTags = metricsConfig.globalTags();
    LOGGER.info("Configuring metrics with common global tags: {}", globalTags);

    // if we have no global tags, use empty (no-op filter)
    if (null == globalTags || globalTags.isEmpty()) {
      return new MeterFilter() {};
    }

    var tags = globalTags.entrySet().stream()
            .map(e -> Tag.of(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

    // Notes from PR 2003: This producer method globalTagsMeterFilter is typically called only once
    // during the application startup phase by the Micrometer extension when it's collecting all the
    // MeterFilter beans. It's not called repeatedly during the application's runtime for every
    // metric being recorded. So no need the cache.
    return MeterFilter.commonTags(Tags.of(tags));
  }

  /**
   * Produces a meter filter to configure distribution statistics for timer metrics such as HTTP
   * server requests, vectorization duration, and reranking calls.
   *
   * <p>
   * Tests in {@link MicrometerConfigurationTests} show what is expected to output for the different
   * metric types.
   *
   * </p>
   * <p>For all distribution metrics, we supress the full histogram buckets to reduce overhead. And then
   * we configure the percentiles based on the metric type:
   *
   * <ul>
   *   <li>Per tenant metrics (see {@link IsPerTenantPredicate}) have fewer percentiles because there will be
   *   many more metrics of these types. </li>
   *   <li>Non per tenant metrics have more percentiles because they are less numerous.</li>
   * </ul>
   * This is applied to all metrics, including the driver metrics.
   *
   * @return A {@link MeterFilter} for configuring distribution statistics.
   */
  @Produces
  @SuppressWarnings("unused")
  public MeterFilter configureDistributionStatistics() {

    final double[] allTenantLatencyPercentiles = {0.5, 0.90, 0.95, 0.98, 0.99};
    final double[] perTenantLatencyPercentiles = {0.5, 0.98, 0.99};
    final Predicate<Meter.Id> isPerTenantPredicate = new IsPerTenantPredicate();

    return new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {

        var builder = DistributionStatisticConfig.builder();
        if (isPerTenantPredicate.test(id)){
          builder = builder.percentiles(perTenantLatencyPercentiles);
        }
        else {
          builder = builder.percentiles(allTenantLatencyPercentiles);
        }

        // make sure we do not publish the histogram buckets for all distribution metrics
        // that can be 70 lines long for a single metric, and we don't need them because we have calc'd
        // the percentiles
        builder = builder.percentilesHistogram(false);

        return builder
            .build()
            .merge(config);
      }
    };
  }

  static class IsPerTenantPredicate implements Predicate<Meter.Id>{

    @Override
    public boolean test(Meter.Id id) {

      // if the Metric has a "tenant" or "session" tag we assume it is a per-tenant metric
      // the API code will use tenant, the Driver uses session
      // getTags() iterates over the tags, but there will never be too many for it to be a problem
      // and sanity check they are not blank strings

      var tenantTag =  id.getTag(TENANT_TAG);
      if (tenantTag != null && !tenantTag.isBlank()) {
        return true;
      }

      var sessionTag =  id.getTag(SESSION_TAG);
      if (sessionTag != null && !sessionTag.isBlank()) {
        return true;
      }
      return false;
    }
  }
}
