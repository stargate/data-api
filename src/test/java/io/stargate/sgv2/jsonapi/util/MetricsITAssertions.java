package io.stargate.sgv2.jsonapi.util;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.Meter;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractDoubleAssert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assertions for working with metrics in **Integration Tests**. */
public class MetricsITAssertions {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetricsITAssertions.class);

  public static final Duration DEFAULT_AWAIT_DURATION = Duration.ofSeconds(60);
  public static final Duration DEFAULT_POLL_DURATION = Duration.ofSeconds(2);

  public static void assertMetricTotal(String metricName, double metricValue) {
    assertMetricTotal(metricName, metricValue, DEFAULT_AWAIT_DURATION, DEFAULT_POLL_DURATION);
  }

  public static void assertMetricTotal(
      String metricName, Consumer<AbstractDoubleAssert<?>> assertConsumer) {
    assertMetricTotal(metricName, Double.NaN, DEFAULT_AWAIT_DURATION, DEFAULT_POLL_DURATION);
  }

  public static void assertMetricTotal(
      String metricName, double metricValue, Duration awaitDuration, Duration pollInterval) {
    assertMetricTotal(metricName, (a) -> a.isEqualTo(metricValue), awaitDuration, pollInterval);
  }

  public static void assertMetricTotal(
      String metricName,
      Consumer<AbstractDoubleAssert<?>> assertConsumer,
      Duration awaitDuration,
      Duration pollInterval) {
    await()
        .atMost(awaitDuration)
        .pollInterval(pollInterval)
        .untilAsserted(
            () -> {
              var asserts =
                  assertThat(metricTotal(metricName))
                      .as("assertMetricTotal() - for metric " + metricName);
              assertConsumer.accept(asserts);
            });
  }

  /**
   * Sum across all tag combinations
   *
   * @param metricName name of the metric to read
   * @return sum of the counter, 0 if the counter is not found.
   */
  public static double metricTotal(String metricName) {

    // e.g. line on /metrics
    //    # TYPE billing_s3_uploaded_events counter
    //    # HELP billing_s3_uploaded_events
    //    billing_s3_uploaded_events_total{module="sgv2-jsonapi"} 30.0
    var metricLines = given().when().get("/metrics").then().statusCode(200).extract().asString();

    AtomicBoolean foundOne = new AtomicBoolean(false);
    var normalizedMetricName = metricName.replace(".", "_");
    var total =
        metricLines
            .lines()
            .filter(line -> line.startsWith(normalizedMetricName))
            .peek(ignored -> foundOne.set(true))
            .mapToDouble(line -> Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1)))
            .sum();

    assertThat(foundOne.get())
        .as("Metric not found. normalizedMetricName:" + normalizedMetricName)
        .isTrue();

    LOGGER.info("metricTotal() - normalizedMetricName: {}, total: {}", normalizedMetricName, total);
    return total;
  }

  public static double metricTotal(Meter.Id meterId) {
    return metricTotal(meterId.getName());
  }

  public static double meterTotal(Meter meter) {
    return metricTotal(meter.getId());
  }
}
