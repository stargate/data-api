package io.stargate.sgv2.jsonapi.testresource;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseDefinitionTransformerV2;
import com.github.tomakehurst.wiremock.http.HttpHeader;
import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.ResponseDefinition;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerModes.Key;
import io.stargate.sgv2.jsonapi.api.v1.findandrerank.FakeRerankerScores;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * Builds the replies of the fake NVIDIA reranker of {@link RerankingTestResource}.
 *
 * <p>The normal reply is HTTP 200 JSON {@code {"rankings": [{"index": i, "logit": x}], "usage":
 * {"prompt_tokens": n, "total_tokens": n}}}, rankings sorted by logit (high first, then index), n
 * the number of passages times {@code tokensPerPassage}. A passage gets its logit from {@link
 * FakeRerankerScores}, else from a leading {@code score:<number>}.
 *
 * <p>A stub selects a special reply with transformer parameters named after {@link Key}; {@link
 * FakeRerankerModes} registers such stubs. The {@code *_ONCE} keys and {@code FAIL_BOTH} count the
 * requests per stub. {@code DELAY_MS}, {@code FAIL_IF_CONTAINS} and {@code REQUIRE_CONCURRENT} are
 * done by the stub and the request journal, not here.
 *
 * <p>A request this class does not recognize gets HTTP 500 {@code {"message":
 * "FAKE_RERANKER_UNRECOGNIZED: <reason>"}}, never an empty success: a body without {@code
 * passages[].text}, or a passage without a score. With {@code modelsByPath} also: not POST, no
 * model on the path, a body without {@code model}, {@code query.text} or {@code truncate}, a model
 * that does not match the path, no {@code Authorization} or {@code tenant-id} header, a mode query
 * that no stub was registered for, or an invalid mode.
 */
public final class FakeRerankTransformer implements ResponseDefinitionTransformerV2 {

  public static final String NAME = "fake-rerank";

  /** Prefix of the message of an unrecognized request. */
  public static final String UNRECOGNIZED = "FAKE_RERANKER_UNRECOGNIZED: ";

  /** Transformer parameter: the mode query that a test stub was registered for. */
  public static final String MODE = "mode";

  private static final String JSON = "application/json";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Map<String, String> modelsByPath;
  private final int tokensPerPassage;
  private final Map<UUID, AtomicInteger> attemptsByStub = new ConcurrentHashMap<>();

  private record Reply(int status, String type, byte[] body, String location) {}

  /**
   * @param modelsByPath each model path with the model name it expects; null skips those checks
   * @param tokensPerPassage the usage tokens reported per passage
   */
  public FakeRerankTransformer(Map<String, String> modelsByPath, int tokensPerPassage) {
    this.modelsByPath = modelsByPath == null ? null : Map.copyOf(modelsByPath);
    this.tokensPerPassage = tokensPerPassage;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean applyGlobally() {
    return false;
  }

  @Override
  public ResponseDefinition transform(ServeEvent serveEvent) {
    Request request = serveEvent.getRequest();
    JsonNode root = parse(request.getBodyAsString());
    List<String> passages = null;
    if (root.path("passages").isArray()) {
      passages = new ArrayList<>();
      for (JsonNode passage : root.get("passages")) {
        passages.add(text(passage.get("text")));
      }
    }
    String query = text(root.path("query").get("text"));
    Parameters parameters = serveEvent.getTransformerParameters();
    int attempt =
        attemptsByStub
            .computeIfAbsent(serveEvent.getStubMapping().getId(), id -> new AtomicInteger())
            .incrementAndGet();
    Integer stubDelay = serveEvent.getResponseDefinition().getFixedDelayMilliseconds();
    int delay = stubDelay == null ? 0 : stubDelay;
    Reply reply;
    try {
      String problem = problem(request, root, passages, query, parameters);
      if (problem != null) {
        reply = unrecognized(problem);
      } else {
        Map<Key, String> mode = mode(parameters);
        delay += attempt == 1 ? number(mode, Key.DELAY_ONCE_MS) : 0;
        Reply shaped = shape(request, mode, passages, attempt);
        String type =
            mode.containsKey(Key.NO_CONTENT_TYPE)
                ? null
                : mode.containsKey(Key.JSON_CHARSET)
                    ? JSON + "; charset=utf-8"
                    : mode.getOrDefault(Key.CONTENT_TYPE, shaped.type());
        reply = new Reply(shaped.status(), type, shaped.body(), shaped.location());
      }
    } catch (RuntimeException e) {
      reply = unrecognized("invalid mode or fake failure: " + e);
    }
    List<HttpHeader> headers = new ArrayList<>();
    if (reply.type() != null) {
      headers.add(new HttpHeader("Content-Type", reply.type()));
    }
    if (reply.location() != null) {
      headers.add(new HttpHeader("Location", reply.location()));
    }
    var response =
        new ResponseDefinitionBuilder()
            .withStatus(reply.status())
            .withHeaders(new HttpHeaders(headers));
    if (reply.body() != null) {
      response.withBody(reply.body());
    }
    if (delay > 0) {
      response.withFixedDelay(delay);
    }
    return response.build();
  }

  private String problem(
      Request request, JsonNode root, List<String> passages, String query, Parameters parameters) {
    boolean noPassages = passages == null || passages.contains(null);
    if (modelsByPath == null) {
      return noPassages ? "the body lacks passages[].text" : null;
    }
    String path = URI.create(request.getUrl()).getPath();
    String expectedModel = modelsByPath.get(path);
    if (!"POST".equals(request.getMethod().getName())) {
      return "method " + request.getMethod() + " is not POST";
    } else if (expectedModel == null) {
      return "no model is configured on path " + path;
    } else if (query == null || text(root.get("truncate")) == null || noPassages) {
      return "the body lacks query.text, passages[].text or truncate";
    } else if (!expectedModel.equals(text(root.get("model")))) {
      return "model %s is not %s, the model on %s"
          .formatted(root.get("model"), expectedModel, path);
    }
    for (String header : List.of("Authorization", "tenant-id")) {
      if (!request.containsHeader(header)) {
        return "missing header " + header;
      }
    }
    if (query.startsWith(FakeRerankerModes.PREFIX)
        && !query.equals(parameters.getString(MODE, null))) {
      return "no stub is registered for the mode query " + query;
    }
    return null;
  }

  /** The reply of the mode for this attempt, before its Content-Type keys are applied. */
  private Reply shape(Request request, Map<Key, String> mode, List<String> passages, int attempt) {
    if (mode.containsKey(Key.STATUS_ONCE) && attempt == 1) {
      return status(number(mode, Key.STATUS_ONCE), "json-message");
    } else if (mode.containsKey(Key.FAIL_BOTH)) {
      String[] codes = mode.get(Key.FAIL_BOTH).split(",");
      return status(Integer.parseInt(codes[attempt == 1 ? 0 : 1].trim()), "json-message");
    } else if (mode.containsKey(Key.STATUS)) {
      return status(number(mode, Key.STATUS), mode.getOrDefault(Key.BODY, "json-message"));
    } else if (mode.containsKey(Key.REDIRECT)) {
      URI target =
          URI.create(request.getAbsoluteUrl()).resolve(RerankingTestResource.REDIRECT_TARGET);
      return new Reply(301, null, null, target.toString());
    } else if (mode.containsKey(Key.NO_BODY)) {
      return new Reply(200, JSON, null, null);
    }
    float[] scores = new float[passages.size()];
    for (int i = 0; i < scores.length; i++) {
      Float score =
          mode.containsKey(Key.HASH_SCORES)
              ? Float.valueOf(FakeRerankerScores.hashScore(passages.get(i)))
              : score(passages.get(i));
      if (score == null) {
        return unrecognized(
            "passage is not in FakeRerankerScores and has no score: \"" + passages.get(i) + "\"");
      }
      scores[i] = score;
    }
    return json(200, rankingsBody(mode, scores));
  }

  /** The logit from the score table, else from a leading {@code score:<number>}, else null. */
  private static Float score(String passage) {
    Float score = FakeRerankerScores.find(passage);
    if (score == null && passage.startsWith("score:")) {
      score = Float.parseFloat(passage.substring(6).split("\\s+", 2)[0]);
    }
    return score;
  }

  private String rankingsBody(Map<Key, String> mode, float[] scores) {
    if (mode.containsKey(Key.EMPTY_OBJECT)) {
      return "{}";
    } else if (mode.containsKey(Key.INVALID_JSON)) {
      return "{\"rankings\": [";
    } else if (mode.containsKey(Key.WRONG_TYPES)) {
      return "{\"rankings\":\"not-an-array\",\"usage\":{\"prompt_tokens\":1,\"total_tokens\":1}}";
    }
    int n = scores.length;
    String field = mode.containsKey(Key.SCORE_FIELD) ? "score" : "logit";
    String[] sizeAndPlus = mode.getOrDefault(Key.INDEX_PLUS_IF_SIZE, "-1,0").split(",");
    int indexPlus =
        n == Integer.parseInt(sizeAndPlus[0].trim()) ? Integer.parseInt(sizeAndPlus[1].trim()) : 0;
    ArrayNode rankings = MAPPER.createArrayNode();
    IntStream.range(0, n)
        .boxed()
        .sorted(Comparator.comparing((Integer i) -> -scores[i]).thenComparing(i -> i))
        .forEach(
            i -> {
              ObjectNode ranking = rankings.addObject();
              if (!mode.containsKey(Key.NO_INDEX)) {
                ranking.put("index", i + indexPlus);
              }
              if (mode.containsKey(Key.LOGIT)) {
                ranking.putRawValue(field, new RawValue(mode.get(Key.LOGIT)));
              } else {
                ranking.put(field, scores[i]);
              }
              if (mode.containsKey(Key.UNKNOWN_FIELDS)) {
                ranking.put("farr_unknown", true);
              }
            });
    if (mode.containsKey(Key.DROP_ONE) && n > 0) {
      rankings.remove(n - 1);
    }
    if (mode.containsKey(Key.EXTRA_ONE)) {
      rankings.addObject().put("index", n).put(field, 0f);
    }
    if (mode.containsKey(Key.DUP_INDEX) && n > 1) {
      ((ObjectNode) rankings.get(1)).set("index", rankings.get(0).get("index"));
    }
    if ((mode.containsKey(Key.INDEX_OUT_OF_RANGE) || mode.containsKey(Key.NEGATIVE_INDEX))
        && n > 0) {
      ((ObjectNode) rankings.get(0)).put("index", mode.containsKey(Key.NEGATIVE_INDEX) ? -1 : n);
    }
    if (mode.containsKey(Key.EMPTY_RANKINGS)) {
      rankings.removeAll();
    }
    int tokens = tokensPerPassage * n;
    ObjectNode usage =
        MAPPER.createObjectNode().put("prompt_tokens", tokens).put("total_tokens", tokens);
    if (mode.containsKey(Key.PARTIAL_USAGE)) {
      String dropped = mode.get(Key.PARTIAL_USAGE);
      if (usage.remove(dropped.isEmpty() ? "total_tokens" : dropped) == null) {
        throw new IllegalArgumentException("PARTIAL_USAGE=" + dropped + " is not a usage field");
      }
    }
    if (mode.containsKey(Key.UNKNOWN_FIELDS)) {
      usage.put("farr_unknown", 1);
    }
    if (mode.containsKey(Key.DUP_KEY)) {
      return "{\"rankings\":%s,\"rankings\":%s,\"usage\":%s}".formatted(rankings, rankings, usage);
    }
    ObjectNode root = MAPPER.createObjectNode();
    if (!mode.containsKey(Key.NO_RANKINGS)) {
      root.set("rankings", rankings);
    }
    if (!mode.containsKey(Key.NO_USAGE)) {
      root.set("usage", usage);
    }
    if (mode.containsKey(Key.UNKNOWN_FIELDS)) {
      root.put("farr_unknown", "ignored");
    }
    return root.toString();
  }

  private static Reply status(int code, String body) {
    String text = "FAKE_RERANKER_STATUS_" + code;
    return switch (body) {
      case "json-message" -> json(code, "{\"message\":\"" + text + "\"}");
      case "json-no-message" -> json(code, "{\"error\":\"" + text + "\"}");
      case "text" -> new Reply(code, "text/plain", text.getBytes(UTF_8), null);
      default -> unrecognized("unknown BODY " + body);
    };
  }

  private static Reply json(int status, String body) {
    return new Reply(status, JSON, body.getBytes(UTF_8), null);
  }

  private static Reply unrecognized(String reason) {
    String body = MAPPER.createObjectNode().put("message", UNRECOGNIZED + reason).toString();
    return new Reply(500, JSON, body.getBytes(UTF_8), null);
  }

  /** The mode keys among the transformer parameters, with their values as text. */
  private static Map<Key, String> mode(Parameters parameters) {
    Map<Key, String> mode = new EnumMap<>(Key.class);
    if (parameters != null) {
      parameters.forEach(
          (name, value) -> {
            if (Arrays.stream(Key.values()).anyMatch(k -> k.name().equals(name))) {
              mode.put(Key.valueOf(name), String.valueOf(value));
            }
          });
    }
    return mode;
  }

  /** The number value of the key, or 0 when the mode does not have the key. */
  private static int number(Map<Key, String> mode, Key key) {
    return mode.containsKey(key) ? Integer.parseInt(mode.get(key).trim()) : 0;
  }

  private static JsonNode parse(String body) {
    try {
      JsonNode root = MAPPER.readTree(body);
      return root != null && root.isObject() ? root : MAPPER.createObjectNode();
    } catch (IOException e) {
      return MAPPER.createObjectNode();
    }
  }

  private static String text(JsonNode node) {
    return node != null && node.isTextual() ? node.textValue() : null;
  }
}
