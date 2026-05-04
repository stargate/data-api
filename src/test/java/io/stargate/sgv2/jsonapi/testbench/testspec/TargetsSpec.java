package io.stargate.sgv2.jsonapi.testbench.testspec;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Spec file that contains targets
 */
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

  public TargetConfiguration getTarget(String targetName) {
    return targets.stream()
        .filter(target -> target.name().equals(targetName))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("target targetName not found: " + targetName));
  }

  public static TargetsSpec loadAll(String path) {
    final Path dir = SpecFiles.resourceDir(path);

    try {
      return MAPPER.readValue(dir.toFile(), TargetsSpec.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
