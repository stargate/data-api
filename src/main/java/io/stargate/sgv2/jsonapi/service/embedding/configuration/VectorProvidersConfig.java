package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import org.eclipse.microprofile.config.spi.ConfigSource;

@StaticInitSafe
public class VectorProvidersConfig
    implements org.eclipse.microprofile.config.spi.ConfigSourceProvider {
  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {
    try {
      URL resourceURL = forClassLoader.getResource("vectorProviders.yaml");
      if (resourceURL != null) {
        YamlConfigSource configSource = new YamlConfigSource(resourceURL);
        return Collections.singletonList(configSource);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to load vectorProviders.yaml", e);
    }
    return Collections.emptyList();
  }
}
