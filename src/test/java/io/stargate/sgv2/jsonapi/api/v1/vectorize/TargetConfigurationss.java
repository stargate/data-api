package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record TargetConfigurationss(List<TargetConfiguration> targets) {

  private static final ObjectMapper MAPPER = new ObjectMapper();


  public TargetConfigurationss {
    Set<String> seen = new HashSet<String>();
    for  (TargetConfiguration target : targets) {
      if (seen.contains(target.name())) {
        throw new IllegalArgumentException("target name already exists: " + target.name() );
      }
      seen.add(target.name());
    }
  }

  public TargetConfiguration configuration(String name) {
    return targets.stream().filter(target -> target.name().equals(name)).findFirst().orElseThrow(() -> new IllegalArgumentException("target name not found: " + name));
  }

  static TargetConfigurationss loadAll(String path) {
    final Path dir = resourceDir(path);

    try {
      return MAPPER.readValue(dir.toFile(), TargetConfigurationss.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Path resourceDir(String path) {
    String normalized = path.startsWith("/") ? path.substring(1) : path;

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    URL url = cl.getResource(normalized);
    if (url == null) {
      throw new IllegalArgumentException("Test resource folder not found: " + path);
    }

    try {
      // Works for file: URLs; if you run tests from a jar, switch to getResourceAsStream-based
      // walking.
      return Paths.get(url.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Bad resource URI for: " + path + " -> " + url, e);
    }
  }

}
