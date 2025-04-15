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
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Centralized configuration of Micrometer {@link MeterFilter}s. Provides filters for applying
 * global tags and configuring distribution statistics (percentiles, histograms, SLA boundaries) for
 * specific metrics.
 */
public class MicrometerConfiguration {

  // Inject necessary configuration beans
  private final MetricsConfig metricsConfig;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  @Inject
  public MicrometerConfiguration(
      MetricsConfig metricsConfig, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.metricsConfig = metricsConfig;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  /**
   * Produces a meter filter that applies configured global tags (e.g. module tag sgv2-jsonapi) to
   * all metrics. Reads tags from {@link MetricsConfig#globalTags()}.
   *
   * @return A {@link MeterFilter} for applying common tags.
   */
  @Produces
  @Singleton
  @jakarta.annotation.Priority(
      0) // Give common tags a higher priority , lower number = higher priority
  public MeterFilter globalTagsMeterFilter() {
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
   * Produces a meter filter that configures distribution statistics (percentiles, histograms, SLAs)
   * for specific timer metrics used throughout the application.
   *
   * <p>Configures:
   *
   * <ul>
   *   <li>HTTP Server Requests (http.server.requests.*): P50, P90, P95, P99, Histogram
   *   <li>Vectorize Call Duration (vectorize.call.duration): P50, P90, P95, P99, Histogram
   *   <li>Tenant Reranking Duration (rerank.tenant.call.duration): P98, Histogram
   *   <li>Overall Reranking Duration (rerank.all.call.duration): P80, P95, P98, Histogram
   * </ul>
   *
   * @return A {@link MeterFilter} for configuring distribution statistics.
   */
  @Produces
  @Singleton
  @jakarta.annotation.Priority(1) // Lower priority than global tags
  public MeterFilter configureDistributionStatistics() {
    return new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(
          Meter.Id id, DistributionStatisticConfig config) {
        String name = id.getName();

        // --- Configuration for HTTP Server & Vectorize metrics ---
        if (name.startsWith(HTTP_SERVER_REQUESTS)
            || name.equals(jsonApiMetricsConfig.vectorizeCallDurationMetrics())) {
          return DistributionStatisticConfig.builder()
              .percentiles(0.5, 0.90, 0.95, 0.99) // Client-side percentiles
              .percentilesHistogram(true) // Enable Prometheus histogram buckets
              .build()
              .merge(config);
        }

        // --- Configuration for Tenant Reranking Timer ---
        else if (name.equals(TENANT_CALL_DURATION_METRIC)) {
          return DistributionStatisticConfig.builder()
              .percentiles(0.98) // Publish only P98 client-side
              .percentilesHistogram(true) // Enable Prometheus histogram buckets
              .build()
              .merge(config);
        }

        // --- Configuration for Overall Reranking Timer ---
        else if (name.equals(ALL_CALL_DURATION_METRIC)) {
          return DistributionStatisticConfig.builder()
              .percentiles(0.80, 0.95, 0.98) // Publish P80, P95, P98 client-side
              .percentilesHistogram(true) // Enable Prometheus histogram buckets
              .build()
              .merge(config);
        }

        // For all other metrics, return the default configuration
        return config;
      }
    };
  }
}
