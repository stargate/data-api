package io.stargate.sgv2.jsonapi.service.provider;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loading the YAML configuration file from the resource folder or file path and making the config
 * available to the application.
 *
 * <ol>
 *   <li>With env {@link
 *       EmbeddingAndRerankingConfigSourceProvider#EMBEDDING_CONFIG_FILE_PATH},{@link
 *       EmbeddingAndRerankingConfigSourceProvider#RERANKING_CONFIG_FILE_PATH} variable set, Data
 *       API loads provider config from specified file location. E.G. Astra Data API.
 *   <li>With system property {@link
 *       EmbeddingAndRerankingConfigSourceProvider#DEFAULT_RERANKING_CONFIG_RESOURCE_OVERRIDE} set,
 *       it will override the provider config resource. E.G. Data API integration test.
 *   <li>With none set, Data API loads default provider config from resource folder. E.G. Local
 *       development.
 * </ol>
 */
@StaticInitSafe
public class EmbeddingAndRerankingConfigSourceProvider implements ConfigSourceProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(EmbeddingAndRerankingConfigSourceProvider.class);

  // Environment variable name to load embedding config from a file path
  private static final String EMBEDDING_CONFIG_FILE_PATH = "EMBEDDING_CONFIG_PATH";
  // Environment variable name to load reranking config from a file path
  private static final String RERANKING_CONFIG_FILE_PATH = "RERANKING_CONFIG_PATH";

  // Default embedding config resource.
  private static final String DEFAULT_EMBEDDING_CONFIG_RESOURCE = "embedding-providers-config.yaml";
  // Default reranking config resource.
  private static final String DEFAULT_RERANKING_CONFIG_RESOURCE = "reranking-providers-config.yaml";
  // System property name to override reranking config resource. Could be set by integration test
  // resource.
  private static final String DEFAULT_RERANKING_CONFIG_RESOURCE_OVERRIDE =
      "DEFAULT_RERANKING_CONFIG_RESOURCE_OVERRIDE";

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
   *   <li>With env variable {@link
   *       EmbeddingAndRerankingConfigSourceProvider#EMBEDDING_CONFIG_FILE_PATH} set, Data API loads
   *       provider config from specified file location. E.G. Data API astra deployment.
   *   <li>If the env is not set, use the default config from the resources folder.
   * </ol>
   */
  private ConfigSource getEmbeddingConfigSources(ClassLoader forClassLoader) throws IOException {
    String filePathFromEnv = System.getenv(EMBEDDING_CONFIG_FILE_PATH);
    if (filePathFromEnv != null) {
      LOGGER.info("Loading embedding config from file path: {}", filePathFromEnv);
      return loadConfigSourceFromFile(filePathFromEnv);
    } else {
      LOGGER.info(
          "Loading embedding config from default resource file : {}",
          DEFAULT_EMBEDDING_CONFIG_RESOURCE);
      return loadConfigSourceFromResource(DEFAULT_EMBEDDING_CONFIG_RESOURCE, forClassLoader);
    }
  }

  /**
   * Method to load the embedding config source.
   *
   * <ol>
   *   <li>With env variable {@link
   *       EmbeddingAndRerankingConfigSourceProvider#RERANKING_CONFIG_FILE_PATH} set, Data API loads
   *       provider config from specified file location. E.G. Data API astra deployment.
   *   <li>With system property {@link
   *       EmbeddingAndRerankingConfigSourceProvider#DEFAULT_RERANKING_CONFIG_RESOURCE_OVERRIDE}
   *       set, it indicated Data API is running for integration tests, then override the default
   *       config resource.
   *   <li>If none is set, use the default config from the resources folder. E.G. Local development
   *       mode.
   * </ol>
   */
  private ConfigSource getRerankingConfigSources(ClassLoader forClassLoader) throws IOException {
    String filePathFromEnv = System.getenv(RERANKING_CONFIG_FILE_PATH);
    String resourceOverride = System.getProperty(DEFAULT_RERANKING_CONFIG_RESOURCE_OVERRIDE);

    if (filePathFromEnv != null) {
      LOGGER.info("Loading reranking config from file path: {}", filePathFromEnv);
      return loadConfigSourceFromFile(filePathFromEnv);
    } else if (resourceOverride != null) {
      LOGGER.info("Loading reranking config from override resource: {}", resourceOverride);
      return loadConfigSourceFromResource("test-reranking-providers-config.yaml", forClassLoader);
    } else {
      LOGGER.info(
          "Loading reranking config from default resource: {}", DEFAULT_RERANKING_CONFIG_RESOURCE);
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
      LOGGER.error("Config file does not exist at the path: {}", file.getCanonicalPath());
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
      LOGGER.error("Resource not found: {}", resource);
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException("Resource not found in: %s", resource);
    }
    return new YamlConfigSource(resourceURL);
  }
}
