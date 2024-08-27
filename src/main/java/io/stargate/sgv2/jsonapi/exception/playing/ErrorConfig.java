package io.stargate.sgv2.jsonapi.exception.playing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Java objects to hold the config from the errors config yaml file for use by {@link ErrorTemplate}
 *
 * <p>See the {@link #DEFAULT_ERROR_CONFIG_FILE} for description of what the yaml should look like.
 *
 * <p>To use this, call {@link #getInstance()} in code, this will cause the yaml file to be loaded
 * if needed. The file will be loaded from {@link #DEFAULT_ERROR_CONFIG_FILE} resource in the JAR.
 * Then use {@link #getErrorDetail(ErrorFamily, String, String)} if you want the template, or the
 * use {@link #getSnippetVars()} when running templates.
 *
 * <p>If you need more control use the {@link #initializeFromYamlResource(String)} before any code
 * causes the config to be read or {@link #unsafeInitializeFromYamlResource(String)} .
 *
 * <p>A {@link IllegalStateException} will be raised if an attempt is made to re-read the config.
 *
 * <p>Using a class rather than a record so we can cache converting the snippets to a map for use in
 * the templates.
 */
public class ErrorConfig {

  /**
   * Method to get the configured ErrorConfig instance that has the data from the file
   *
   * @return
   */
  public static ErrorConfig getInstance() {
    return CACHE.get(CACHE_KEY);
  }

  private final List<Snippet> snippets;
  private final List<ErrorDetail> requestErrors;
  private final List<ErrorDetail> serverErrors;

  // Lazy loaded map  of the snippets for use in the templates
  private Map<String, String> snippetVars;

  // Prefix used when adding snippets to the variables for a template.
  public static final String SNIPPET_VAR_PREFIX = "SNIPPET.";

  // TIDY: move this to the config sections
  public static final String DEFAULT_ERROR_CONFIG_FILE = "errors.yaml";

  @JsonCreator
  public ErrorConfig(
      @JsonProperty("snippets") List<Snippet> snippets,
      @JsonProperty("request-errors") List<ErrorDetail> requestErrors,
      @JsonProperty("server-errors") List<ErrorDetail> serverErrors) {

    // defensive immutable copies because this config can be shared
    this.snippets = snippets == null ? List.of() : List.copyOf(snippets);
    this.requestErrors = requestErrors == null ? List.of() : List.copyOf(requestErrors);
    this.serverErrors = serverErrors == null ? List.of() : List.copyOf(serverErrors);
  }

  /**
   * ErrorDetail is a record to hold the template for a particular error.
   *
   * <p>Does not have {@link ErrorFamily} because we have that as the only hierarchy in the config
   * file, it is two different lists in the parent class.
   *
   * @param scope
   * @param code
   * @param title
   * @param body
   * @param httpResponseOverride Optional override for the HTTP response code for this error, only
   *     needs to be set if different from {@link APIException#DEFAULT_HTTP_RESPONSE}. <b>NOTE:</b>
   *     there is no checking that this is a well known HTTP status code, as we do not want to
   *     depend on classes like {@link jakarta.ws.rs.core.Response.Status} in this class and if we
   *     want to return a weird status this class should not limit that. It would be handled higher
   *     up the stack and tracked with Integration Tests.
   */
  public record ErrorDetail(
      String scope,
      String code,
      String title,
      String body,
      Optional<Integer> httpResponseOverride) {

    public ErrorDetail {
      if (scope == null) {
        scope = ErrorScope.NONE.scope();
      }
      // scope can be empty, if not empty must be snake case
      if (!scope.isBlank()) {
        requireSnakeCase(scope, "scope");
      }

      Objects.requireNonNull(code, "code cannot be null");
      requireSnakeCase(code, "code");

      Objects.requireNonNull(title, "title cannot be null");
      if (title.isBlank()) {
        throw new IllegalArgumentException("title cannot be blank");
      }

      Objects.requireNonNull(body, "body cannot be null");
      if (body.isBlank()) {
        throw new IllegalArgumentException("body cannot be blank");
      }

      Objects.requireNonNull(httpResponseOverride, "httpResponseOverride cannot be null");
    }
  }

  /**
   * Snippet is a record to hold the template for a particular snippet.
   *
   * <p>
   *
   * @param name
   * @param body
   */
  public record Snippet(String name, String body) {

    public Snippet {
      Objects.requireNonNull(name, "name cannot be null");
      requireSnakeCase(name, "name");

      Objects.requireNonNull(body, "body cannot be null");
      if (body.isBlank()) {
        throw new IllegalArgumentException("body cannot be blank");
      }
    }

    /**
     * Name to use for this snippet when substituting into templates.
     *
     * @return
     */
    public String variableName() {
      return SNIPPET_VAR_PREFIX + name;
    }
  }

  /**
   * See {@link #getSnippetVars()} for the cached map of snippets vars.
   *
   * @return
   */
  public List<Snippet> snippets() {
    return snippets;
  }

  /**
   * See {@link #getErrorDetail(ErrorFamily, String, String)} to get a template for a specific error
   * code.
   *
   * @return
   */
  public List<ErrorDetail> requestErrors() {
    return requestErrors;
  }

  /**
   * See {@link #getErrorDetail(ErrorFamily, String, String)} to get a template for a specific error
   * code.
   *
   * @return
   */
  public List<ErrorDetail> serverErrors() {
    return serverErrors;
  }

  /**
   * Helper to optionally get a {@link ErrorDetail} for use by a {@link ErrorTemplate}
   *
   * @param family
   * @param scope
   * @param code
   * @return
   */
  public Optional<ErrorDetail> getErrorDetail(ErrorFamily family, String scope, String code) {

    var errors =
        switch (family) {
          case REQUEST -> requestErrors;
          case SERVER -> serverErrors;
        };
    return errors.stream()
        .filter(e -> e.scope().equals(scope) && e.code().equals(code))
        .findFirst();
  }

  /**
   * Returns a map of the snippets for use in the templates.
   *
   * <p>The map is cached, recommend us this rather than call {@link #snippets()} for every error
   *
   * @return
   */
  public Map<String, String> getSnippetVars() {

    if (snippetVars == null) {
      // NOTE: Potential race condition, should be OK because the data won't change and we are only
      // writing.
      // want the map to be immutable because we hand it out
      snippetVars =
          Map.copyOf(
              snippets.stream().collect(Collectors.toMap(s -> s.variableName(), Snippet::body)));
    }
    return snippetVars;
  }

  // Reusable Pattern for UPPER_SNAKE_CASE_2 - allows alpha and digits
  private static final Pattern UPPER_SNAKE_CASE_PATTERN =
      Pattern.compile("^[A-Z0-9]+(_[A-Z0-9]+)*$");

  private static void requireSnakeCase(String value, String name) {

    if (!UPPER_SNAKE_CASE_PATTERN.matcher(Strings.nullToEmpty(value)).matches()) {
      throw new IllegalArgumentException(
          name + " must be in UPPER_SNAKE_CASE_1 format, got: " + value);
    }
  }

  // there is a single item in the cache
  private static final String CACHE_KEY = "key";

  // Using a caffine cache even though there is a single instance of the ErrorConfig read from disk
  // so we can either lazy load when we first need it using default file or load from a
  // different file or yaml string for tests etc.
  private static final LoadingCache<String, ErrorConfig> CACHE =
      Caffeine.newBuilder().build(key -> readFromYamlResource(DEFAULT_ERROR_CONFIG_FILE));

  private static ErrorConfig maybeCacheErrorConfig(ErrorConfig errorConfig) {

    if (CACHE.getIfPresent(CACHE_KEY) != null) {
      throw new IllegalStateException("ErrorConfig already initialised");
    }
    CACHE.put(CACHE_KEY, errorConfig);
    return errorConfig;
  }

  @VisibleForTesting
  /**
   * Configures a new {@link ErrorConfig} using the data in the YAML string.
   *
   * <p><b>NOTE:</b> does not "initialize" the class, just creates a new instance with the data from
   * the YAML string. Use the initializeFrom methods for that, or let the default behaviour kickin.
   */
  public static ErrorConfig readFromYamlString(String yaml) throws JsonProcessingException {

    // This is only going to happen once at system start, ok to create a new mapper
    ObjectMapper mapper = new YAMLMapper();
    // module for Optional support see
    // https://github.com/FasterXML/jackson-modules-java8/tree/2.18/datatypes
    mapper.registerModule(new Jdk8Module());
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
    return mapper.readValue(yaml, ErrorConfig.class);
  }

  /**
   * Create a new {@link ErrorConfig} using the YAML contents of the resource file, does not
   * initialise. This is for use by the cache loader.
   *
   * @param path
   * @return
   * @throws IOException
   */
  private static ErrorConfig readFromYamlResource(String path) throws IOException {

    URL resourceURL = ErrorConfig.class.getClassLoader().getResource(path);
    if (resourceURL == null) {
      throw new FileNotFoundException("ErrorConfig Resource not found: " + path);
    }
    URI resourceURI;
    try {
      resourceURI = resourceURL.toURI();
    } catch (URISyntaxException e) {
      throw new IOException("ErrorConfig Resource " + path + " has Invalid URI: " + resourceURL, e);
    }
    return readFromYamlString(Files.readString(Paths.get(resourceURI)));
  }

  /**
   * Create a new {@link ErrorConfig} using the YAML contents of the resource file and initialise it
   * as the static config.
   *
   * @param path
   * @return
   * @throws IOException
   */
  public static ErrorConfig initializeFromYamlResource(String path) throws IOException {
    return maybeCacheErrorConfig(readFromYamlResource(path));
  }

  /**
   * Clears the current config and replaces it with the file at <code>path</code>. <b>DO NOT USE</b>
   * in regular code, only for testing.
   *
   * @param path
   * @return
   * @throws IOException
   */
  @VisibleForTesting
  public static ErrorConfig unsafeInitializeFromYamlResource(String path) throws IOException {
    CACHE.invalidate(CACHE_KEY);
    return initializeFromYamlResource(path);
  }
}
