package io.stargate.sgv2.jsonapi.api.v1.findandrerank;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.util.IntegrationTestUtils;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.service.embedding.operation.test.CustomITEmbeddingProvider;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Builds request headers and findAndRerank command JSON. Example: {@code
 * findAndRerank().hybrid("cheese").option("rerankOn", "title").json()} gives {@code
 * {"findAndRerank":{"sort":{"$hybrid":"cheese"},"options":{"rerankOn":"title"}}}}. Tests that send
 * a documentation example, malformed JSON or duplicate keys write the JSON text instead.
 */
public final class FindAndRerankRequests {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private FindAndRerankRequests() {}

  /** The Data API token that the test base classes send in the {@code Token} header. */
  public static String dataApiToken() {
    Base64.Encoder base64 = Base64.getEncoder();
    return "Cassandra:"
        + base64.encodeToString(IntegrationTestUtils.getCassandraUsername().getBytes())
        + ":"
        + base64.encodeToString(IntegrationTestUtils.getCassandraPassword().getBytes());
  }

  /**
   * The default headers, {@code Token} and the {@code x-embedding-api-key} that the test embedding
   * provider requires, changed by name/value pairs: a non-null value adds or replaces a header, a
   * null value removes it. Example: {@code headers("reranking-api-key", "my-key")}. The defaults
   * have no {@code reranking-api-key}, so the reranker receives {@code Bearer <Data API token>}.
   */
  public static Map<String, Object> headers(Object... nameValuePairs) {
    Map<String, Object> headers = new LinkedHashMap<>();
    headers.put(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, dataApiToken());
    headers.put(
        HttpConstants.EMBEDDING_AUTHENTICATION_TOKEN_HEADER_NAME,
        CustomITEmbeddingProvider.TEST_API_KEY);
    for (int i = 0; i < nameValuePairs.length; i += 2) {
      if (nameValuePairs[i + 1] == null) {
        headers.remove((String) nameValuePairs[i]);
      } else {
        headers.put((String) nameValuePairs[i], nameValuePairs[i + 1]);
      }
    }
    return headers;
  }

  /** Starts a findAndRerank command. */
  public static Builder findAndRerank() {
    return new Builder();
  }

  /** Builder for {@code {"findAndRerank": {...}}}; parts appear in the order they are set. */
  public static final class Builder {
    private final ObjectNode body = MAPPER.createObjectNode();

    private Builder() {}

    /** Sets {@code filter} from JSON text. */
    public Builder filter(String json) {
      body.set("filter", parse(json));
      return this;
    }

    /** Sets {@code projection} from JSON text. */
    public Builder projection(String json) {
      body.set("projection", parse(json));
      return this;
    }

    /** Sets {@code sort} from JSON text, for example {@code {"$hybrid": {"$vectorize": "x"}}}. */
    public Builder sort(String json) {
      body.set("sort", parse(json));
      return this;
    }

    /** Sets {@code sort} to {@code {"$hybrid": text}}. */
    public Builder hybrid(String text) {
      body.putObject("sort").put("$hybrid", text);
      return this;
    }

    /** Adds every field of this JSON object to {@code options}. */
    public Builder options(String json) {
      options().setAll((ObjectNode) parse(json));
      return this;
    }

    /** Sets one option; the value may be a String, number, Boolean, Map, List or null. */
    public Builder option(String name, Object value) {
      options().set(name, MAPPER.valueToTree(value));
      return this;
    }

    /** Returns the command JSON. */
    public String json() {
      return MAPPER.createObjectNode().set("findAndRerank", body).toString();
    }

    private ObjectNode options() {
      return body.has("options") ? (ObjectNode) body.get("options") : body.putObject("options");
    }
  }

  /** Parses JSON text; fails the test on invalid JSON. */
  public static JsonNode parse(String json) {
    try {
      return MAPPER.readTree(json);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Not valid JSON: " + json, e);
    }
  }
}
