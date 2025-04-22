/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.jsonapi.api.v1.metrics;

import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.HttpMetrics.HTTP_SERVER_REQUESTS;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics.ALL_CALL_DURATION_METRIC;
import static io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics.TENANT_CALL_DURATION_METRIC;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.smallrye.config.SmallRyeConfig;
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
 *   <li>Apply global tags (e.g., module identifier) to all metrics based on {@link MetricsConfig}.
 *   <li>Configure distribution statistics (client-side percentiles, Prometheus histogram buckets,
 *       etc.) for specific timer metrics used throughout the application, such as HTTP server
 *       requests, vectorization duration, and reranking durations.
 * </ul>
 */
public final class MicrometerConfiguration {

  // Private constructor to prevent instantiation
  private MicrometerConfiguration() {}

  /**
   * Produces a meter filter that applies configured global tags (e.g., {@code module=sgv2-jsonapi})
   * to all metrics. Reads tags from {@link MetricsConfig#globalTags()}.
   *
   * @return A {@link MeterFilter} for applying common tags, or an empty filter if no global tags
   *     are configured.
   */
  @Produces
  public MeterFilter globalTagsMeterFilter() {
    MetricsConfig metricsConfig =
        ConfigProvider.getConfig()
            .unwrap(SmallRyeConfig.class)
            .getConfigMapping(MetricsConfig.class);

    Map<String, String> globalTags = metricsConfig.globalTags();

    // if we have no global tags, use empty
    if (null == globalTags || globalTags.isEmpty()) {
      return new MeterFilter() {};
    }

    // transform to tags
    Collection<Tag> tags =
        globalTags.entrySet().stream()
            .map(e -> Tag.of(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

    // return all
    return MeterFilter.commonTags(Tags.of(tags));
  }

  /**
   * Produces a meter filter that configures distribution statistics for specific timer metrics used
   * throughout the application..
   *
   * <p>This filter enables Prometheus-compatible histogram buckets for server-side percentile
   * calculation (via {@code histogram_quantile}) and configures specific client-side percentiles
   * where required.
   *
   * <p>Configures:
   *
   * <ul>
   *   <li>HTTP Server Requests {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.HttpMetrics#HTTP_SERVER_REQUESTS}:
   *       P50, P90, P95, P99, Histogram
   *   <li>Vectorize Call Duration {@link JsonApiMetricsConfig#vectorizeCallDurationMetrics()}: P50,
   *       P90, P95, P99, Histogram
   *   <li>Tenant Reranking Duration {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics#TENANT_CALL_DURATION_METRIC}:
   *       P98, Histogram
   *   <li>Overall Reranking Duration {@value
   *       io.stargate.sgv2.jsonapi.config.constants.MetricsConstants.RerankingMetrics#ALL_CALL_DURATION_METRIC}:
   *       P80, P95, P98, Histogram
   * </ul>
   *
   * @return A {@link MeterFilter} for configuring distribution statistics.
   */
  @Produces
  public MeterFilter configureDistributionStatistics() {
    return new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(
          Meter.Id id, DistributionStatisticConfig config) {
        JsonApiMetricsConfig jsonApiMetricsConfig =
            ConfigProvider.getConfig()
                .unwrap(SmallRyeConfig.class)
                .getConfigMapping(JsonApiMetricsConfig.class);

        String name = id.getName();
        DistributionStatisticConfig.Builder builder = null;

        // --- Configuration for HTTP Server & Vectorize metrics ---
        if (name.startsWith(HTTP_SERVER_REQUESTS)
            || name.equals(jsonApiMetricsConfig.vectorizeCallDurationMetrics())) {
          builder = DistributionStatisticConfig.builder().percentiles(0.5, 0.90, 0.95, 0.99);
        }

        // --- Configuration for Tenant Reranking Timer ---
        else if (name.equals(TENANT_CALL_DURATION_METRIC)) {
          builder = DistributionStatisticConfig.builder().percentiles(0.98);
        }

        // --- Configuration for Overall Reranking Timer ---
        else if (name.equals(ALL_CALL_DURATION_METRIC)) {
          builder = DistributionStatisticConfig.builder().percentiles(0.80, 0.95, 0.98);
        }

        // If a specific configuration was found, enable histograms and merge
        if (builder != null) {
          // Enable Prometheus histogram buckets for all configured timers
          // This allows server-side histogram_quantile calculations
          builder.percentilesHistogram(true);
          return builder.build().merge(config);
        }

        // For all other metrics, return the default configuration
        return config;
      }
    };
  }
}
