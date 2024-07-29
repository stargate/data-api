package io.stargate.sgv2.jsonapi.exception.playing;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.source.yaml.YamlConfigSource;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.eclipse.microprofile.config.spi.ConfigSourceProvider;

/** Loading the YAML configuration file from the resource folder */
@StaticInitSafe
public class ErrorCodeConfigSourceProvider implements ConfigSourceProvider {
  private static final String DEFAULT_ERROR_FILE = "errors.yaml";

  @Override
  public Iterable<ConfigSource> getConfigSources(ClassLoader forClassLoader) {
    try {
      // Load the YAML files from src/main/resources/
      URL resourceURL = forClassLoader.getResource(DEFAULT_ERROR_FILE);
      YamlConfigSource configSource = new YamlConfigSource(resourceURL);
      return Collections.singletonList(configSource);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load embedding provider config from errors.yaml, ", e);
    }
  }
}
