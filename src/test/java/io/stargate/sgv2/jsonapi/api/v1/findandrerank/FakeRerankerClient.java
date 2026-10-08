package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Timing;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.Key;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankTransformer;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankerTestResource.Model;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Test-side access to the fake reranker: register special replies, read what it received and clear
 * its state. It uses the WireMock admin API at the URL that {@link FakeRerankerTestResource}
 * publishes in the system property {@link FakeRerankerTestResource#URL_PROPERTY}.
 */
public final class FakeRerankerClient {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** How long after its delay an unanswered request counts as given up by the caller. */
  private static final long GIVE_UP_MILLIS = 500;

  private static String adminUrl;
  private static WireMock admin;

  private FakeRerankerClient() {}

  /** Removes the stubs of {@link #stubMode(String)} and clears the recorded requests. */
  public static void reset() {
    admin().removeStubsByMetadataPattern(matchingJsonPath("$." + FakeRerankerModes.TEST_STUB));
    admin().resetRequests();
  }

  /** Registers the reply that a {@link FakeRerankerModes} query names; a plain query is a no-op. */
  public static void stubMode(String query) {
    if (query.startsWith(FakeRerankerModes.PREFIX)) {
      FakeRerankerModes.stubs(query).forEach(admin()::register);
    }
  }

  /** Every request received since the last reset, in arrival order. */
  public static List<RecordedRerankRequest> requests() {
    // The journal lists the newest request first; requests in the same millisecond keep that order.
    List<ServeEvent> arrived = new ArrayList<>(admin().getServeEvents());
    Collections.reverse(arrived);
    arrived.sort(Comparator.comparingLong(e -> e.getRequest().getLoggedDate().getTime()));
    List<RecordedRerankRequest> requests = new ArrayList<>();
    for (int i = 0; i < arrived.size(); i++) {
      requests.add(record(i + 1, arrived.get(i), arrived));
    }
    return requests;
  }

  /** The requests received on the endpoint of this model, in arrival order. */
  public static List<RecordedRerankRequest> requests(Model model) {
    return requests().stream().filter(r -> Objects.equals(r.path(), model.path())).toList();
  }

  /**
   * Waits until the fake has answered every request it recorded, at most 15 s, so that a delayed
   * reply cannot reach the journal of the next test. A request whose caller gave up (no reply was
   * sent {@link #GIVE_UP_MILLIS} after its delay) counts as answered. Fails when the fake is still
   * busy after 15 s.
   */
  public static void awaitIdle() {
    long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
    List<ServeEvent> busy = busy();
    while (!busy.isEmpty() && System.nanoTime() < deadline) {
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
      busy = busy();
    }
    if (!busy.isEmpty()) {
      throw new IllegalStateException("The fake reranker is still handling " + busy);
    }
  }

  private static List<ServeEvent> busy() {
    long now = System.currentTimeMillis();
    return admin().getServeEvents().stream()
        .filter(e -> e.getTiming().getResponseSendTime() == null)
        .filter(e -> now < e.getRequest().getLoggedDate().getTime() + delay(e) + GIVE_UP_MILLIS)
        .toList();
  }

  private static RecordedRerankRequest record(int seq, ServeEvent event, List<ServeEvent> all) {
    var request = event.getRequest();
    Map<String, String> headers = new LinkedHashMap<>();
    for (HttpHeader header : request.getHeaders().all()) {
      headers.put(header.key().toLowerCase(Locale.ROOT), String.join(",", header.values()));
    }
    JsonNode root = parse(request.getBodyAsString());
    List<String> passages = null;
    if (root.path("passages").isArray()) {
      passages = new ArrayList<>();
      for (JsonNode passage : root.get("passages")) {
        passages.add(text(passage.get("text")));
      }
    }
    String query = text(root.path("query").get("text"));
    long arrival = request.getLoggedDate().getTime();
    Boolean concurrent = null;
    if (query != null
        && query.startsWith(FakeRerankerModes.PREFIX)
        && FakeRerankerModes.parse(query).containsKey(Key.REQUIRE_CONCURRENT)) {
      // Another request arrived while this one was held, before its reply could be sent.
      long heldUntil = arrival + delay(event);
      concurrent =
          all.stream()
              .filter(other -> other != event)
              .map(other -> other.getRequest().getLoggedDate().getTime())
              .anyMatch(time -> time >= arrival && time < heldUntil);
    }
    return new RecordedRerankRequest(
        seq,
        arrival,
        request.getMethod().getName(),
        URI.create(request.getUrl()).getPath(),
        headers,
        request.getBodyAsString(),
        text(root.get("model")),
        query,
        passages,
        text(root.get("truncate")),
        event.getResponse().getStatus(),
        unrecognized(event),
        concurrent);
  }

  /** Why the fake did not recognize the request, or null. */
  private static String unrecognized(ServeEvent event) {
    if (!event.getWasMatched()) {
      return "no stub matched "
          + event.getRequest().getMethod()
          + " "
          + event.getRequest().getUrl();
    }
    String message = text(parse(event.getResponse().getBodyAsString()).get("message"));
    return message != null && message.startsWith(FakeRerankTransformer.UNRECOGNIZED)
        ? message.substring(FakeRerankTransformer.UNRECOGNIZED.length())
        : null;
  }

  private static long delay(ServeEvent event) {
    Timing timing = event.getTiming();
    return timing == null || timing.getAddedDelay() == null ? 0 : timing.getAddedDelay();
  }

  private static synchronized WireMock admin() {
    String baseUrl = System.getProperty(FakeRerankerTestResource.URL_PROPERTY);
    if (baseUrl == null) {
      throw new IllegalStateException("No fake reranker: the class needs FakeRerankerTestResource");
    }
    if (!baseUrl.equals(adminUrl)) {
      URI uri = URI.create(baseUrl);
      admin = new WireMock(uri.getHost(), uri.getPort());
      adminUrl = baseUrl;
    }
    return admin;
  }

  private static JsonNode parse(String body) {
    try {
      JsonNode root = body == null ? null : MAPPER.readTree(body);
      return root != null && root.isObject() ? root : MAPPER.createObjectNode();
    } catch (Exception e) {
      return MAPPER.createObjectNode();
    }
  }

  private static String text(JsonNode node) {
    return node != null && node.isTextual() ? node.textValue() : null;
  }
}
