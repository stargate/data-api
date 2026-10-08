package io.stargate.sgv2.jsonapi.testresource;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Test resource for the findAndRerank integration tests that need a working reranker.
 *
 * <p>It starts the database like {@link DseTestResource}, then starts the fake reranker of {@link
 * RerankingTestResource#startFakeReranker} (WireMock, in the test JVM) and points every reranking
 * model URL at it, so no test calls the external development reranker named in {@code
 * test-reranking-providers-config.yaml}. It also adds the models and providers of {@link Model},
 * sets nvidia {@code NONE.enabled=false} (the code only checks that the key exists), sets the
 * {@code hybridLimits} configuration default to 5 (bounds stay 1 and 100), sets the default read
 * page size to 5, and allows 12 collections and 150 indexes. These are the only differences from
 * the configuration that the other integration tests use; each one except the page size (see below)
 * is needed by some test.
 *
 * <p>The page size of 5 is below the 12 and 50 documents that the batching tests read, but it does
 * not show which page size a findAndRerank read uses: on HCD those tests pass even when the reads
 * take their page size from this setting instead of from {@code hybridLimits}, because the vector
 * read still returns every row up to its limit in the first page. DSE was not tried. A side effect:
 * a plain {@code find} returns at most 5 documents per page here.
 *
 * <p>Use it with {@code @QuarkusTestResource(value = ..., restrictToAnnotatedClass = true)} on a
 * class without {@code @Nested} classes. The fake's base URL, which also serves the WireMock admin
 * API, is published in the test JVM system property {@link #URL_PROPERTY}. The application must run
 * on the same host as the tests, as it does with the jar launch used locally and in CI.
 */
public class FakeRerankerTestResource extends DseTestResource {

  /** Test JVM system property with the fake reranker base URL. */
  public static final String URL_PROPERTY = "findandrerank.fake-reranker.url";

  public static final String PROVIDERS = "stargate.jsonapi.reranking.providers.";
  public static final String VECTOR_LIMIT =
      "stargate.jsonapi.operations.hybrid-search-vector-limit";
  public static final String LEXICAL_LIMIT =
      "stargate.jsonapi.operations.hybrid-search-lexical-limit";

  /**
   * Every reranking model the tests can name. The first three come from the YAML file and only get
   * their URL replaced. The others are appended to the nvidia list (indexes 3 and up) or are the
   * only model of an extra provider. Settings are {@code key=value} pairs separated by {@code ;}:
   * {@code url}, {@code status} (api-model-support status) or a request property such as {@code
   * max-batch-size} (default 10). {@link #path()} is the model's endpoint on the fake, or null when
   * the URL deliberately does not reach the fake.
   */
  public enum Model {
    DEFAULT("nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", "nvidia-default", ""),
    DEPRECATED("nvidia", "nvidia/a-random-deprecated-model", "nvidia-deprecated", ""),
    EOL("nvidia", "nvidia/a-random-EOL-model", "nvidia-eol", ""),
    /** Second working model, batch size 3. */
    SECOND("nvidia", "nvidia/farr-second-model", "nvidia-second", "max-batch-size=3"),
    /** Read timeout 500 ms, 1 retry, back-off 10 to 20 ms. */
    FAST_TIMEOUT(
        "nvidia",
        "nvidia/farr-fast-timeout",
        "nvidia-fast-timeout",
        "read-timeout-millis=500;at-most-retries=1;"
            + "initial-back-off-millis=10;max-back-off-millis=20"),
    BAD_URL("nvidia", "nvidia/farr-bad-url", null, "url=http://bad host/rerank"),
    NEGATIVE_BATCH(
        "nvidia", "nvidia/farr-negative-batch", "nvidia-negative-batch", "max-batch-size=-1"),
    ZERO_RETRIES("nvidia", "nvidia/farr-zero-retries", "nvidia-zero-retries", "at-most-retries=0"),
    DEPRECATED_NO_MESSAGE(
        "nvidia", "nvidia/farr-deprecated-no-message", "nvidia-deprecated-nm", "status=DEPRECATED"),
    EOL_NO_MESSAGE("nvidia", "nvidia/farr-eol-no-message", "nvidia-eol-nm", "status=END_OF_LIFE"),
    /**
     * URL on a local port that was opened and closed again, so nothing listens there. 3 s back-off
     * without jitter, so a retry is visible in the timing.
     */
    CONNECTION_REFUSED(
        "nvidia",
        "nvidia/farr-connection-refused",
        null,
        "initial-back-off-millis=3000;max-back-off-millis=3000;jitter=0"),
    /**
     * Host name in the reserved top level domain {@code .invalid}, which never resolves. 3 s
     * back-off without jitter, so a retry is visible in the timing.
     */
    UNKNOWN_HOST(
        "nvidia",
        "nvidia/farr-unknown-host",
        null,
        "url=http://farr-no-such-host.invalid/x;initial-back-off-millis=3000;"
            + "max-back-off-millis=3000;jitter=0"),
    /**
     * Same name as DEFAULT, listed later, END_OF_LIFE: a lookup by name that takes the later entry
     * fails with END_OF_LIFE_AI_MODEL, so requests should never reach it.
     */
    DUPLICATE(
        "nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", "nvidia-duplicate", "status=END_OF_LIFE"),
    /** Back-off 200 ms doubling up to 1600 ms, no jitter. */
    GROWING_BACK_OFF(
        "nvidia",
        "nvidia/farr-growing-back-off",
        "nvidia-growing-back-off",
        "initial-back-off-millis=200;max-back-off-millis=1600;jitter=0"),
    /** Back-off 100 ms capped at 150 ms, no jitter, 6 retries. */
    CAPPED_BACK_OFF(
        "nvidia",
        "nvidia/farr-capped-back-off",
        "nvidia-capped-back-off",
        "initial-back-off-millis=100;max-back-off-millis=150;jitter=0;at-most-retries=6"),
    /** Provider in the ModelProvider enum without a reranking client; model named like DEFAULT. */
    VOYAGE("voyageAI", "nvidia/llama-3.2-nv-rerankqa-1b-v2", "voyage", ""),
    /** Provider whose name is not in the ModelProvider enum. */
    FAKE_PROVIDER("fakeProvider", "fake/farr-model", "fake-provider", ""),
    /** Model of a disabled provider. */
    JINA("jinaAI", "jina/farr-model", "jina", ""),
    /** Model of a provider that only supports SHARED_SECRET authentication. */
    COHERE("cohere", "cohere/farr-model", "cohere", ""),
    /** Model of a provider that only supports HEADER authentication. */
    MISTRAL("mistral", "mistral/farr-model", "mistral", "");

    private final String provider;
    private final String modelName;
    private final String path;
    private final String settings;

    Model(String provider, String modelName, String path, String settings) {
      this.provider = provider;
      this.modelName = modelName;
      this.path = path;
      this.settings = settings;
    }

    public String provider() {
      return provider;
    }

    public String modelName() {
      return modelName;
    }

    /** The request path on the fake, for example {@code /rerank/nvidia-second}, or null. */
    public String path() {
      return path == null ? null : RerankingTestResource.RERANK_PATH + path;
    }

    /** True for the three models from the YAML file. */
    public boolean fromYaml() {
      return ordinal() < 3;
    }
  }

  /** Provider settings of the extra providers: enabled, display name, authentication type. */
  private static final Map<String, String[]> EXTRA_PROVIDERS =
      Map.of(
          "voyageAI", new String[] {"true", "FARR voyage", "NONE"},
          "fakeProvider", new String[] {"true", "FARR fake provider", "NONE"},
          "jinaAI", new String[] {"false", "FARR disabled jina", "NONE"},
          "cohere", new String[] {"true", "FARR shared secret only", "SHARED_SECRET"},
          "mistral", new String[] {"true", "FARR header only", "HEADER"});

  private WireMockServer server;

  @Override
  public int getMaxCollectionsPerDBOverride() {
    return 12;
  }

  @Override
  public int getIndexesPerDBOverride() {
    return 150;
  }

  @Override
  public Map<String, String> start() {
    long begin = System.currentTimeMillis();
    Map<String, String> props = new LinkedHashMap<>(super.start());
    long databaseMillis = System.currentTimeMillis() - begin;
    Map<String, String> modelsByPath = new HashMap<>();
    Arrays.stream(Model.values())
        .filter(m -> m.path() != null)
        .forEach(m -> modelsByPath.put(m.path(), m.modelName()));
    server = RerankingTestResource.startFakeReranker(modelsByPath);
    String baseUrl = "http://127.0.0.1:" + server.port();
    props.putAll(rerankerConfig(baseUrl));
    props.putAll(overrides());
    System.setProperty(URL_PROPERTY, baseUrl);
    // One line per application start, so the starts and the database start time can be read from
    // the Maven log.
    System.out.printf(
        "FARR_FAKE_RERANKER_STARTED resource=%s url=%s databaseStartMillis=%d props=%s%n",
        getClass().getSimpleName(), baseUrl, databaseMillis, props);
    return props;
  }

  @Override
  public void stop() {
    if (server != null) {
      server.stop();
      System.out.printf("FARR_FAKE_RERANKER_STOPPED resource=%s%n", getClass().getSimpleName());
    }
    super.stop();
  }

  /** Configuration that a subclass puts on top of the entries of this class. */
  protected Map<String, String> overrides() {
    return Map.of();
  }

  private static Map<String, String> rerankerConfig(String baseUrl) {
    Map<String, String> props = new LinkedHashMap<>();
    Map<String, Integer> nextIndex = new HashMap<>(Map.of("nvidia", 3));
    for (Model model : Model.values()) {
      int index =
          model.fromYaml() ? model.ordinal() : nextIndex.merge(model.provider, 1, Integer::sum) - 1;
      String prefix = PROVIDERS + model.provider + ".models[" + index + "].";
      Map<String, String> settings = new LinkedHashMap<>();
      if (!model.fromYaml()) {
        settings.put("name", model.modelName);
        settings.put("properties.max-batch-size", "10");
        String[] provider = EXTRA_PROVIDERS.get(model.provider);
        if (provider != null) {
          props.put(PROVIDERS + model.provider + ".enabled", provider[0]);
          props.put(PROVIDERS + model.provider + ".display-name", provider[1]);
          props.put(
              PROVIDERS + model.provider + ".supported-authentications." + provider[2] + ".enabled",
              "true");
        }
      }
      settings.put(
          "url",
          model.path() != null
              ? baseUrl + model.path()
              : model == Model.CONNECTION_REFUSED ? closedPortUrl() : null);
      for (String pair : model.settings.split(";")) {
        if (!pair.isEmpty()) {
          String[] kv = pair.split("=", 2);
          String key =
              switch (kv[0]) {
                case "url" -> "url";
                case "status" -> "api-model-support.status";
                default -> "properties." + kv[0];
              };
          settings.put(key, kv[1]);
        }
      }
      settings.forEach((key, value) -> props.put(prefix + key, value));
    }
    props.put(PROVIDERS + "nvidia.supported-authentications.NONE.enabled", "false");
    props.put(VECTOR_LIMIT, "1,5,100");
    props.put(LEXICAL_LIMIT, "1,5,100");
    props.put("stargate.jsonapi.operations.default-page-size", "5");
    return props;
  }

  /** A URL on a local port that nothing listens on: the port is opened and closed right away. */
  private static String closedPortUrl() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return "http://127.0.0.1:" + socket.getLocalPort() + "/rerank";
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
