package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;

/**
 * Loading the YAML configuration file from the resource folder or env variable and making the
 * config available to the application.
 */
@StaticInitSafe
public class EmbeddingConfigSourceProvider implements ConfigSourceProvider {
  String configPath = System.getenv("EMBEDDING_CONFIG_PATH");

  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {
    try {
      // Load the YAML files from environment variable
      if (configPath != null && !configPath.isEmpty()) {
        File file = new File(configPath);
        if (!file.exists()) {
          throw new RuntimeException("Config file does not exist at the path: " + configPath);
        }
        URL fileUrl = new File(configPath).toURI().toURL();
        YamlConfigSource configSource = new YamlConfigSource(fileUrl);
        return Collections.singletonList(configSource);
      }
      // Load the YAML files from src/main/resources/
      URL resourceURL = forClassLoader.getResource("embedding-providers-config.yaml");
      YamlConfigSource configSource = new YamlConfigSource(resourceURL);
      return Collections.singletonList(configSource);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to load embedding provider config from embedding-providers-config.yaml, ", e);
    }
  }
}
