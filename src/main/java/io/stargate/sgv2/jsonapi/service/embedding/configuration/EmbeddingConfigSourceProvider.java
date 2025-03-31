package io.stargate.sgv2.jsonapi.service.embedding.configuration;

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
 * Loading the YAML configuration file from the resource folder or env variable and making the
 * config available to the application.
 *
 * <ul>
 *   <li>With env variables set, Data API loads provider config from specified resource location.
 *   <li>With system properties set, Data API loads provider config from specified resource
 *       location.
 *   <li>Without env variables or system properties set, Data API loads provider config from
 *       resource folder.
 * </ul>
 *
 * >
 */
@StaticInitSafe
public class EmbeddingConfigSourceProvider implements ConfigSourceProvider {
  private static final String EMBEDDING_CONFIG_ENV = "EMBEDDING_CONFIG_PATH";
  private static final String RERANKING_CONFIG_ENV = "RERANKING_CONFIG_PATH";
  private static final String EMBEDDING_CONFIG_RESOURCE = "embedding-providers-config.yaml";
  private static final String RERANKING_CONFIG_RESOURCE = "reranking-providers-config.yaml";

  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {

    var embeddingSource =
        System.getenv(EMBEDDING_CONFIG_ENV) == null
            ? System.getProperty(EMBEDDING_CONFIG_ENV)
            : System.getenv(EMBEDDING_CONFIG_ENV);
    var rerankingSource =
        System.getenv(RERANKING_CONFIG_ENV) == null
            ? System.getProperty(RERANKING_CONFIG_ENV)
            : System.getenv(RERANKING_CONFIG_ENV);

    List<ConfigSource> configSources = new ArrayList<>();
    try {
      // Add embedding config source
      configSources.add(
          loadConfigSource(embeddingSource, EMBEDDING_CONFIG_RESOURCE, forClassLoader));

      // Add reranking config source
      configSources.add(
          loadConfigSource(rerankingSource, RERANKING_CONFIG_RESOURCE, forClassLoader));

      return configSources;
    } catch (IOException e) {
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          e, "Failed to load configuration: %s", e.getMessage());
    }
  }

  /**
   * Loads a config source from the provided environment path if it exists, otherwise falls back to
   * the resource path.
   *
   * @param envPath Path from environment variable
   * @param resourcePath Path to resource file
   * @param classLoader ClassLoader to load resources
   * @return The loaded YamlConfigSource
   * @throws IOException If loading fails
   */
  private YamlConfigSource loadConfigSource(
      String envPath, String resourcePath, ClassLoader classLoader) throws IOException {
    // Try to load from environment path first
    if (envPath != null && !envPath.isEmpty()) {
      File file = new File(envPath);
      if (!file.exists()) {
        throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
            "Config file does not exist at the path: %s", file.getCanonicalPath());
      }
      return new YamlConfigSource(file.toURI().toURL());
    }

    // Fall back to resource path
    URL resourceURL = classLoader.getResource(resourcePath);
    if (resourceURL == null) {
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          "Resource not found: %s", resourcePath);
    }
    return new YamlConfigSource(resourceURL);
  }
}
