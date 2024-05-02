package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;

@StaticInitSafe
public class EmbeddingConfigSourceProvider implements ConfigSourceProvider {
  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {
    try {
      URL resourceURL = forClassLoader.getResource("embedding-providers-config.yaml");
      YamlConfigSource configSource = new YamlConfigSource(resourceURL);
      return Collections.singletonList(configSource);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load vector provider config", e);
    }
  }
}
