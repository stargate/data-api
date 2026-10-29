package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.common.Metadata;
import io.stargate.sgv2.jsonapi.testresource.FakeRerankTransformer;
import io.stargate.sgv2.jsonapi.testresource.RerankingTestResource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Query texts that name a special reply of the fake reranker. findAndRerank sends {@code
 * rerankQuery} to the reranker unchanged, so a test puts a mode string there, for example {@code
 * mode(Key.STATUS_ONCE, 408)}, and registers it with {@link FakeRerankerClient#stubMode(String)}:
 * the stub gives that reply to the requests whose {@code query.text} is the mode string. A mode
 * string is {@code FAKE:} followed by {@code KEY} or {@code KEY=VALUE} parts separated by {@code ;}
 * (values cannot contain {@code ;}). Any other query is the normal mode, scored from {@link
 * FakeRerankerScores}; a mode query without a registered stub makes the request "unrecognized". The
 * "first request" of the {@code *_ONCE} and {@code FAIL_BOTH} keys is counted per registered stub.
 */
public final class FakeRerankerModes {

  /** Every mode string starts with this prefix. */
  public static final String PREFIX = "FAKE:";

  /**
   * Metadata attribute that marks the stubs of a test, which {@link FakeRerankerClient} removes.
   */
  static final String TEST_STUB = "farrTestStub";

  /** The keys; "=x" in a comment means the key takes a value. */
  public enum Key {
    HASH_SCORES, // score each passage with FakeRerankerScores.hashScore, no table lookup
    STATUS, // =code: reply with this HTTP status; BODY picks the body
    BODY, // =json-message (default), json-no-message or text: the body for STATUS
    STATUS_ONCE, // =code: the first request for this query gets this status, later ones are normal
    DELAY_MS, // =millis: wait, then reply normally
    DELAY_ONCE_MS, // =millis: only the first request for this query waits
    REQUIRE_CONCURRENT, // record whether another request arrived while this one was held
    CONTENT_TYPE, // =value: reply with this Content-Type
    JSON_CHARSET, // reply with Content-Type application/json; charset=utf-8 (values cannot hold ;)
    NO_CONTENT_TYPE, // reply without a Content-Type header
    NO_BODY, // HTTP 200 with Content-Type application/json and no body
    SCORE_FIELD, // put scores in "score" instead of "logit"
    NO_USAGE, // leave out "usage"
    PARTIAL_USAGE, // =field (optional): "usage" without this field; default "total_tokens"
    EMPTY_OBJECT, // the body is {}
    NO_RANKINGS, // leave out "rankings"
    EMPTY_RANKINGS, // "rankings": []
    DROP_ONE, // drop the last ranking
    EXTRA_ONE, // add a ranking whose index is the number of passages
    DUP_INDEX, // the second ranking repeats the index of the first one
    INDEX_OUT_OF_RANGE, // the first ranking's index is the number of passages
    NEGATIVE_INDEX, // the first ranking's index is -1
    INDEX_PLUS_IF_SIZE, // =size,n: add n to every index, only in a request with size passages
    NO_INDEX, // leave out "index" in every ranking
    UNKNOWN_FIELDS, // add unknown fields to the body, to every ranking and to "usage"
    INVALID_JSON, // the body is truncated JSON
    WRONG_TYPES, // "rankings" is a string
    DUP_KEY, // the body has the "rankings" key twice, with the same value
    REDIRECT, // HTTP 301 with Location pointing at the fake's redirect target
    LOGIT, // =raw JSON number: every logit is written as this text, for example 1e39
    FAIL_IF_CONTAINS, // =passage: a batch with this exact passage gets HTTP 500, others are normal
    FAIL_BOTH // =a,b: the first request for this query gets status a, every later one status b
  }

  /** The bodies for {@link Key#STATUS}. */
  public enum Body {
    JSON_MESSAGE, // {"message": "FAKE_RERANKER_STATUS_<code>"}, application/json
    JSON_NO_MESSAGE, // {"error": "FAKE_RERANKER_STATUS_<code>"}, application/json
    TEXT // FAKE_RERANKER_STATUS_<code>, text/plain
  }

  private FakeRerankerModes() {}

  /** {@code FAKE:<KEY>}, for example {@code mode(Key.NO_USAGE)}. */
  public static String mode(Key key) {
    return PREFIX + key.name();
  }

  /** {@code FAKE:<KEY>=<value>}, for example {@code mode(Key.STATUS_ONCE, 408)}. */
  public static String mode(Key key, Object value) {
    return PREFIX + key.name() + "=" + value;
  }

  /** {@code FAKE:STATUS=<code>;BODY=<body>}. */
  public static String status(int code, Body body) {
    String bodyValue = body.name().toLowerCase(Locale.ROOT).replace('_', '-');
    return combine(mode(Key.STATUS, code), mode(Key.BODY, bodyValue));
  }

  /** Joins modes into one, for example {@code combine(mode(HASH_SCORES), mode(DELAY_MS, 100))}. */
  public static String combine(String... modes) {
    return PREFIX
        + Arrays.stream(modes)
            .map(m -> m.startsWith(PREFIX) ? m.substring(PREFIX.length()) : m)
            .collect(Collectors.joining(";"));
  }

  /** The keys and values of a mode string; an unknown key fails. */
  static Map<Key, String> parse(String mode) {
    Map<Key, String> keys = new EnumMap<>(Key.class);
    for (String part : mode.substring(PREFIX.length()).split(";")) {
      String[] keyValue = part.split("=", 2);
      keys.put(Key.valueOf(keyValue[0]), keyValue.length > 1 ? keyValue[1] : "");
    }
    return keys;
  }

  /**
   * The stubs for a mode string: requests on a model path whose {@code query.text} is the mode get
   * the reply of {@link FakeRerankTransformer}, with the mode keys as parameters. {@code DELAY_MS}
   * becomes the stub's fixed delay; {@code FAIL_IF_CONTAINS} adds a stub with a higher priority for
   * requests with that passage, which get HTTP 500; {@code REQUIRE_CONCURRENT} is read from the
   * request journal, see {@link FakeRerankerClient#requests()}.
   */
  static List<MappingBuilder> stubs(String mode) {
    Map<Key, String> keys = parse(mode);
    Map<String, Object> parameters = new HashMap<>();
    keys.forEach((key, value) -> parameters.put(key.name(), value));
    List.of(Key.DELAY_MS, Key.FAIL_IF_CONTAINS, Key.REQUIRE_CONCURRENT)
        .forEach(key -> parameters.remove(key.name()));
    parameters.put(FakeRerankTransformer.MODE, mode);
    List<MappingBuilder> stubs = new ArrayList<>();
    stubs.add(stub(mode, 3, parameters, keys));
    if (keys.containsKey(Key.FAIL_IF_CONTAINS)) {
      Map<String, Object> failing = new HashMap<>(parameters);
      failing.remove(Key.BODY.name());
      failing.put(Key.STATUS.name(), "500");
      String passage = keys.get(Key.FAIL_IF_CONTAINS).replace("'", "\\'");
      stubs.add(
          stub(mode, 2, failing, keys)
              .withRequestBody(matchingJsonPath("$.passages[?(@.text == '" + passage + "')]")));
    }
    return stubs;
  }

  private static MappingBuilder stub(
      String mode, int priority, Map<String, Object> parameters, Map<Key, String> keys) {
    var response =
        aResponse()
            .withTransformers(FakeRerankTransformer.NAME)
            .withTransformerParameters(parameters);
    if (keys.containsKey(Key.DELAY_MS)) {
      response.withFixedDelay(Integer.parseInt(keys.get(Key.DELAY_MS).trim()));
    }
    return any(urlPathMatching(RerankingTestResource.RERANK_PATH + ".+"))
        .atPriority(priority)
        .withRequestBody(matchingJsonPath("$.query.text", equalTo(mode)))
        .withMetadata(Metadata.metadata().attr(TEST_STUB, true).build())
        .willReturn(response);
  }
}
