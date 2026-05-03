package io.stargate.sgv2.jsonapi.testbench.testspec;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public record TargetsSpec(TestSpecMeta meta, List<TargetConfiguration> targets)
    implements TestSpec {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);

  public TargetsSpec {
    Set<String> seen = new HashSet<String>();
    for (TargetConfiguration target : targets) {
      if (seen.contains(target.name())) {
        throw new IllegalArgumentException("target name already exists: " + target.name());
      }
      seen.add(target.name());
    }
  }

  public TargetConfiguration configuration(String name) {
    return targets.stream()
        .filter(target -> target.name().equals(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("target name not found: " + name));
  }

  public static TargetsSpec loadAll(String path) {
    final Path dir = resourceDir(path);

    try {
      return MAPPER.readValue(dir.toFile(), TargetsSpec.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Path resourceDir(String path) {
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
