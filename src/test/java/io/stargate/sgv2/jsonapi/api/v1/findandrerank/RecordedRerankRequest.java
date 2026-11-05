package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * One request received by the fake reranker, from {@link FakeRerankerClient#requests()}. WireMock
 * records a request before its delay, so a request the caller gave up on is still recorded. Fields
 * parsed from the body are null when the body lacks them.
 *
 * @param seq 1-based arrival number since the last reset
 * @param arrivalMillis when the fake received the request, in epoch milliseconds of the test JVM
 * @param headers request headers; names in lower case, repeated values joined with a comma
 * @param status HTTP status of the reply
 * @param unrecognized why the fake did not recognize the request, or null
 * @param concurrent for {@code REQUIRE_CONCURRENT}: whether another request arrived while this one
 *     was held
 */
public record RecordedRerankRequest(
    int seq,
    long arrivalMillis,
    String method,
    String path,
    Map<String, String> headers,
    String body,
    String model,
    String query,
    List<String> passages,
    String truncate,
    int status,
    String unrecognized,
    Boolean concurrent) {

  /** Returns a request header by name, ignoring case, or null. */
  public String header(String name) {
    return headers == null ? null : headers.get(name.toLowerCase(Locale.ROOT));
  }

  @Override
  public String toString() {
    return "#%d %s %s status=%d model=%s query=%s passages=%s auth=%s tenant=%s unrecognized=%s"
        .formatted(
            seq,
            method,
            path,
            status,
            model,
            query,
            passages,
            header("Authorization"),
            header("tenant-id"),
            unrecognized);
  }
}
