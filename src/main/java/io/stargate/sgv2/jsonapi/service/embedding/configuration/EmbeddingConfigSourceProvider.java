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
 * Loading the YAML configuration file from the resource folder or env/system_property variable and
 * making the config available to the application.
 *
 * <p>variable are: {@link EmbeddingConfigSourceProvider#EMBEDDING_CONFIG_PATH} and {@link
 * EmbeddingConfigSourceProvider#RERANKING_CONFIG_PATH}
 *
 * <ul>
 *   <li>With env variable set, Data API loads provider config from specified file location. E.G.
 *       Astra Data API.
 *   <li>With system property set, Data API loads provider config from specified resource location.
 *       E.G. Data API integration test.
 *   <li>Without env variable or system property set, Data API loads provider config from resource
 *       folder. E.G. Local development.
 * </ul>
 */
@StaticInitSafe
public class EmbeddingConfigSourceProvider implements ConfigSourceProvider {
  private static final String EMBEDDING_CONFIG_PATH = "EMBEDDING_CONFIG_PATH";
  private static final String RERANKING_CONFIG_PATH = "RERANKING_CONFIG_PATH";

  private static final String DEFAULT_EMBEDDING_CONFIG_RESOURCE = "embedding-providers-config.yaml";
  private static final String DEFAULT_RERANKING_CONFIG_RESOURCE = "reranking-providers-config.yaml";

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

  private ConfigSource getEmbeddingConfigSources(ClassLoader forClassLoader) throws IOException {
    // 1. Astra Data API deployment, an environment variable is set to load the config from the file
    // mounted to the pod.
    String filePathFromEnv = System.getenv(EMBEDDING_CONFIG_PATH);
    // 2. If env is not set, use the default config from the resources folder.

    if (filePathFromEnv != null) {
      return loadConfigSourceFromFile(filePathFromEnv);
    } else {
      return loadConfigSourceFromResource(DEFAULT_EMBEDDING_CONFIG_RESOURCE, forClassLoader);
    }
  }

  private ConfigSource getRerankingConfigSources(ClassLoader forClassLoader) throws IOException {

    // 1. Astra Data API deployment, an environment variable is set to load the config from the file
    // mounted to the pod.
    String filePathFromEnv = System.getenv(RERANKING_CONFIG_PATH);
    // 2. Data API integration test, a system property is set to load the config from the test
    // resources folder.
    String resourceUrlFromSystemProperty = System.getProperty(RERANKING_CONFIG_PATH);
    // 3. If both are not set, use the default config from the resources folder.

    if (filePathFromEnv != null) {
      return loadConfigSourceFromFile(filePathFromEnv);
    } else if (resourceUrlFromSystemProperty != null) {
      return loadConfigSourceFromResource(resourceUrlFromSystemProperty, forClassLoader);
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
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          "Config file does not exist at the path: %s", file.getCanonicalPath());
    }
    return new YamlConfigSource(file.toURI().toURL());
  }

  /**
   * Loads a config source from the provided resource path.
   *
   * <ul>
   *   <li>From the resource folder if no env variable or system property is set.
   *   <li>From the system property if set. This is useful for integration tests to specify the test
   *       configuration yaml file.
   * </ul>
   *
   * @param resource Resource path to load
   * @param classLoader ClassLoader to load resources
   * @return The loaded YamlConfigSource
   */
  private YamlConfigSource loadConfigSourceFromResource(String resource, ClassLoader classLoader)
      throws IOException {
    URL resourceURL = classLoader.getResource(resource);
    if (resourceURL == null) {
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException("Resource not found in: %s", resource);
    }
    return new YamlConfigSource(resourceURL);
  }
}
