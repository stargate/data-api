package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.source.yaml.YamlConfigSource;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;

/**
 * Loading the YAML configuration file from the resource folder or env variable and making the
 * config available to the application.
 */
@StaticInitSafe
public class EmbeddingConfigSourceProvider implements ConfigSourceProvider {
  String embeddingConfigPath = System.getenv("EMBEDDING_CONFIG_PATH");
  String rerankConfigPath = System.getenv("RERANK_CONFIG_PATH");

  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {
    List<ConfigSource> configSources = new ArrayList<>();
    try {
      // TODO(Hazel): Change
      // Load the YAML files from environment variable
      if (embeddingConfigPath != null && !embeddingConfigPath.isEmpty()) {
        File file = new File(embeddingConfigPath);
        if (!file.exists()) {
          throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
              "Config file does not exist at the path: %s", file.getCanonicalPath());
        }
        URL fileUrl = file.toURI().toURL();
        YamlConfigSource configSource = new YamlConfigSource(fileUrl);
        return Collections.singletonList(configSource);
      }
      // Load the YAML files from src/main/resources/
      URL resourceURL = forClassLoader.getResource("embedding-providers-config.yaml");
      URL rerankResourceURL = forClassLoader.getResource("rerank-providers-config.yaml");
      YamlConfigSource configSource = new YamlConfigSource(resourceURL);
      YamlConfigSource rerankConfigSource = new YamlConfigSource(rerankResourceURL);
      configSources.add(configSource);
      configSources.add(rerankConfigSource);
      return configSources;
    } catch (IOException e) {
      throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
          e,
          "Failed to load embedding provider config from 'embedding-providers-config.yaml': %s",
          e.getMessage());
    }
  }
}
