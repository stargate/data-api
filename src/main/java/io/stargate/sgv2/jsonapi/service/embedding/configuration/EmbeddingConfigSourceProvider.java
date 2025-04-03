package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.logging.Log;
import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.source.yaml.YamlConfigSource;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;

/**
 * Loading the YAML configuration file from the resource folder or file path and making the config
 * available to the application.
 *
 * <ol>
 *   <li>With env {@link EmbeddingConfigSourceProvider#EMBEDDING_CONFIG_PATH},{@link
 *       EmbeddingConfigSourceProvider#RERANKING_CONFIG_PATH} variable set, Data API loads provider
 *       config from specified file location. E.G. Astra Data API.
 *   <li>With system property {@link EmbeddingConfigSourceProvider#DATA_API_INTEGRATION_TEST} set,
 *       Data API loads test provider config resource. E.G. Data API integration test.
 *   <li>With none set, Data API loads default provider config from resource folder. E.G. Local
 *       development.
 * </ol>
 */
@StaticInitSafe
public class EmbeddingConfigSourceProvider implements ConfigSourceProvider {
  private static final String EMBEDDING_CONFIG_PATH = "EMBEDDING_CONFIG_PATH";
  private static final String RERANKING_CONFIG_PATH = "RERANKING_CONFIG_PATH";

  private static final String DEFAULT_EMBEDDING_CONFIG_RESOURCE = "embedding-providers-config.yaml";
  private static final String DEFAULT_RERANKING_CONFIG_RESOURCE = "reranking-providers-config.yaml";
  private static final String TEST_RERANKING_CONFIG_RESOURCE =
      "test-reranking-providers-config.yaml";

  public static final String DATA_API_INTEGRATION_TEST = "DATA_API_INTEGRATION_TEST";

  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {
    List<ConfigSource> configSources = new ArrayList<>();
    try {
      configSources.add(getEmbeddingConfigSources(forClassLoader));
      configSources.add(getRerankingConfigSources(forClassLoader));
      return configSources;
    } catch (IOException e) {
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          e, "Failed to load configuration: %s", e.getMessage());
    }
  }

  /**
   * Method to load the reranking config source.
   *
   * <ol>
   *   <li>With env variable {@link EmbeddingConfigSourceProvider#RERANKING_CONFIG_PATH} set, Data
   *       API loads provider config from specified file location. E.G. Data API astra deployment.
   *   <li>If the env is not set, use the default config from the resources folder.
   * </ol>
   */
  private ConfigSource getEmbeddingConfigSources(ClassLoader forClassLoader) throws IOException {
    String filePathFromEnv = System.getenv(EMBEDDING_CONFIG_PATH);
    if (filePathFromEnv != null) {
      return loadConfigSourceFromFile(filePathFromEnv);
    } else {
      return loadConfigSourceFromResource(DEFAULT_EMBEDDING_CONFIG_RESOURCE, forClassLoader);
    }
  }

  /**
   * Method to load the embedding config source.
   *
   * <ol>
   *   <li>With env variable {@link EmbeddingConfigSourceProvider#EMBEDDING_CONFIG_PATH} set, Data
   *       API loads provider config from specified file location. E.G. Data API astra deployment.
   *   <li>With system property {@link EmbeddingConfigSourceProvider#DATA_API_INTEGRATION_TEST} set,
   *       it indicated Data API is running for integration tests, then load the test config file
   *       resource.
   *   <li>If none is set, use the default config from the resources folder. E.G. Local development
   *       mode.
   * </ol>
   */
  private ConfigSource getRerankingConfigSources(ClassLoader forClassLoader) throws IOException {
    String filePathFromEnv = System.getenv(RERANKING_CONFIG_PATH);
    String isIT = System.getProperty(DATA_API_INTEGRATION_TEST);

    if (filePathFromEnv != null) {
      return loadConfigSourceFromFile(filePathFromEnv);
    } else if (isIT != null) {
      return loadConfigSourceFromResource(TEST_RERANKING_CONFIG_RESOURCE, forClassLoader);
    } else {
      return loadConfigSourceFromResource(DEFAULT_RERANKING_CONFIG_RESOURCE, forClassLoader);
    }
  }

  /**
   * Loads a config source from the provided file path.
   *
   * @param envPath Path from environment variable
   * @return The loaded YamlConfigSource
   */
  private YamlConfigSource loadConfigSourceFromFile(String envPath) throws IOException {
    File file = new File(envPath);
    if (!file.exists()) {
      Log.error("Config file does not exist at the path: " + file.getCanonicalPath());
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          "Config file does not exist at the path: %s", file.getCanonicalPath());
    }
    return new YamlConfigSource(file.toURI().toURL());
  }

  /**
   * Loads a config source from the provided resource path.
   *
   * @param resource Resource path to load
   * @param classLoader ClassLoader to load resources
   * @return The loaded YamlConfigSource
   */
  private YamlConfigSource loadConfigSourceFromResource(String resource, ClassLoader classLoader)
      throws IOException {
    URL resourceURL = classLoader.getResource(resource);
    if (resourceURL == null) {
      Log.error("Resource not found: " + resource);
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException("Resource not found in: %s", resource);
    }
    return new YamlConfigSource(resourceURL);
  }
}
