package io.stargate.sgv2.jsonapi.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.*;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MicrometerConfigurationTests {
  private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerConfigurationTests.class);

  private static final JsonFactory JSON_FACTORY =
      JsonFactory.builder().enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES).build();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(JSON_FACTORY);

  @Test
  public void allTenantTimerPercentiles() {

    var registry = newRegistry();
    var timer = registry.timer("all.tenant.metric", "tag1", "value1");
    fillMetric(timer);

    var expected =
        """
    all_tenant_metric_seconds{tag1="value1",quantile="0.5",} 1.040154624
    all_tenant_metric_seconds{tag1="value1",quantile="0.9",} 1.81190656
    all_tenant_metric_seconds{tag1="value1",quantile="0.95",} 1.946124288
    all_tenant_metric_seconds{tag1="value1",quantile="0.98",} 2.013233152
    all_tenant_metric_seconds{tag1="value1",quantile="0.99",} 2.013233152
    all_tenant_metric_seconds_count{tag1="value1",} 5120.0
    all_tenant_metric_seconds_sum{tag1="value1",} 5180.291
    all_tenant_metric_seconds_max{tag1="value1",} 2.0
    """;
    assertPublishing(registry, expected, "all tenant timer");
  }

  @Test
  public void perTenantTimerPercentiles() {

    var registry = newRegistry();
    var timer = registry.timer("per.tenant.metric", "tag1", "value1", "tenant", "1234");
    fillMetric(timer);

    var expected =
        """
    per_tenant_metric_seconds{tag1="value1",tenant="1234",quantile="0.5",} 1.040154624
    per_tenant_metric_seconds{tag1="value1",tenant="1234",quantile="0.98",} 2.013233152
    per_tenant_metric_seconds{tag1="value1",tenant="1234",quantile="0.99",} 2.013233152
    per_tenant_metric_seconds_count{tag1="value1",tenant="1234",} 5120.0
    per_tenant_metric_seconds_sum{tag1="value1",tenant="1234",} 5180.291
    per_tenant_metric_seconds_max{tag1="value1",tenant="1234",} 2.0
    """;
    assertPublishing(registry, expected, "per tenant timer");
  }

  @Test
  public void perSessionTimerPercentiles() {

    var registry = newRegistry();
    var timer = registry.timer("per.session.metric", "tag1", "value1", "session", "1234");
    fillMetric(timer);

    var expected =
        """
    per_session_metric_seconds{session="1234",tag1="value1",quantile="0.5",} 1.040154624
    per_session_metric_seconds{session="1234",tag1="value1",quantile="0.98",} 2.013233152
    per_session_metric_seconds{session="1234",tag1="value1",quantile="0.99",} 2.013233152
    per_session_metric_seconds_count{session="1234",tag1="value1",} 5120.0
    per_session_metric_seconds_sum{session="1234",tag1="value1",} 5180.291
    per_session_metric_seconds_max{session="1234",tag1="value1",} 2.0
    """;
    assertPublishing(registry, expected, "per session timer");
  }

  @Test
  public void perTenantDistributionSummary() {

    var registry = newRegistry();
    var metric = registry.summary("per.tenant.metric", "tag1", "value1", "tenant", "1234");
    fillMetric(metric);

    var expected =
        """
    per_tenant_metric_count{tag1="value1",tenant="1234",}
    per_tenant_metric_max{tag1="value1",tenant="1234",}
    per_tenant_metric_sum{tag1="value1",tenant="1234",}
    per_tenant_metric{tag1="value1",tenant="1234",quantile="0.5",}
    per_tenant_metric{tag1="value1",tenant="1234",quantile="0.98",}
    per_tenant_metric{tag1="value1",tenant="1234",quantile="0.99",}
    """;
    assertPublishing(registry, expected, "per tenant summary");
  }

  @Test
  public void perSessionDistributionSummary() {

    var registry = newRegistry();
    var metric = registry.summary("per.tenant.metric", "tag1", "value1", "session", "1234");
    fillMetric(metric);

    var expected =
        """
    per_tenant_metric_count{session="1234",tag1="value1",}
    per_tenant_metric_max{session="1234",tag1="value1",}
    per_tenant_metric_sum{session="1234",tag1="value1",}
    per_tenant_metric{session="1234",tag1="value1",quantile="0.5",}
    per_tenant_metric{session="1234",tag1="value1",quantile="0.98",}
    per_tenant_metric{session="1234",tag1="value1",quantile="0.99",}
    """;
    assertPublishing(registry, expected, "per tenant summary");
  }

  @Test
  public void allTenantDistributionSummary() {

    var registry = newRegistry();
    var metric = registry.summary("per.tenant.metric", "tag1", "value1");
    fillMetric(metric);

    var expected =
        """
    per_tenant_metric_count{tag1="value1",}
    per_tenant_metric_max{tag1="value1",}
    per_tenant_metric_sum{tag1="value1",}
    per_tenant_metric{tag1="value1",quantile="0.5",}
    per_tenant_metric{tag1="value1",quantile="0.9",}
    per_tenant_metric{tag1="value1",quantile="0.95",}
    per_tenant_metric{tag1="value1",quantile="0.98",}
    per_tenant_metric{tag1="value1",quantile="0.99",}
    """;
    assertPublishing(registry, expected, "per tenant summary");
  }

  private void assertPublishing(
      PrometheusMeterRegistry registry, String expectedLines, String context) {

    var actual = cleanMetricLines(registry.scrape());
    var expected = cleanMetricLines(expectedLines);

    assertThat(actual).as(context).isEqualTo(expected);
  }

  private PrometheusMeterRegistry newRegistry() {
    var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    registry.config().meterFilter(new MicrometerConfiguration().configureDistributionStatistics());
    return registry;
  }

  private void fillMetric(Timer timer) {
    // record some values so there is data to be published
    for (int i = 0; i < (1024 * 5); i++) {
      long value = ThreadLocalRandom.current().nextLong(1, 2001); // 1 to 2000 ms
      timer.record(value, TimeUnit.MILLISECONDS);
    }
  }

  private void fillMetric(DistributionSummary distributionSummary) {
    // record some values so there is data to be published
    for (int i = 0; i < (1024 * 5); i++) {
      double value = ThreadLocalRandom.current().nextDouble(1, 2001); // 1 to 2000 ms
      distributionSummary.record(value);
    }
  }

  private String cleanMetricLines(String input) {

    // # is used for comments, remove those lines and remove the value which is after the  " " at
    // end
    // e.g. all_tenant_metric_seconds{tag1="value1",quantile="0.5",} 1.040154624
    // and then sort for comparison
    return input
        .lines()
        .filter(line -> !line.startsWith("#"))
        .map(
            line -> {
              int lastBrace = line.lastIndexOf('}');
              int lastSpace = line.lastIndexOf(' ');
              return (lastBrace >= 0 && lastSpace > lastBrace)
                  ? line.substring(0, lastSpace)
                  : line;
            })
        .sorted()
        .collect(Collectors.joining("\n"));
  }

  @ParameterizedTest
  @MethodSource("testIsPerTenantPredicateArgs")
  public void testIsPerTenantPredicateArgs(String slug, boolean isTenant) {

    var predicate = new MicrometerConfiguration.IsPerTenantPredicate();
    var id = slugToId(slug);
    assertThat(predicate.test(id))
        .as("isTenant=%s for slug %s", isTenant, slug)
        .isEqualTo(isTenant);
  }

  private static Stream<Arguments> testIsPerTenantPredicateArgs() {
    return Stream.of(
        Arguments.of(
            """
            http_server_requests_seconds_bucket{method="POST",module="sgv2-jsonapi",outcome="SUCCESS",status="200",tenant="5d9bf1c5-bead-48ec-ac04-6662c2ae9cff",uri="/v1/{keyspace}/{collection}",user_agent="astrapy",le="2.505397588"}""",
            true),
        Arguments.of(
            """
                session_cql_requests_seconds{module="sgv2-jsonapi",session="default_tenant",quantile="0.98"}""",
            true),
        Arguments.of(
            "cache_gets_total{cache=\"cql_sessions_cache\",module=\"sgv2-jsonapi\",result=\"hit\"} ",
            false));
  }

  /**
   * Pass in a metric from the /metrics and it will create the ID e.g.
   *
   * <pre>
   *   http_server_requests_seconds_bucket{method="POST",module="sgv2-jsonapi",outcome="SUCCESS",status="200",tenant="5d9bf1c5-bead-48ec-ac04-6662c2ae9cff",uri="/v1/{keyspace}/{collection}",user_agent="astrapy",le="2.505397588"}
   * </pre>
   */
  private Meter.Id slugToId(String slug) {

    int braceIndex = slug.indexOf('{');
    String name = slug.substring(0, braceIndex);
    String metadata = slug.substring(braceIndex).replaceAll("=", ":");

    Map<String, String> rawTags = Map.of();
    try {
      rawTags = OBJECT_MAPPER.readValue(metadata, new TypeReference<Map<String, String>>() {});

    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    var tags =
        Tags.of(
            rawTags.entrySet().stream()
                .map(e -> Tag.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList()));
    LOGGER.info("slugToId - slug {} -> name {} tags {}", slug, name, tags);

    return new Meter.Id(name, tags, null, null, Meter.Type.OTHER);
  }
}
