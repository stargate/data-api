package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Named vector-index profiles: each maps a profile name to a set of Cassandra SAI indexing options.
 * Selecting a profile is an alternative to passing raw options through {@code vectorIndexing}.
 *
 * <p>Profiles never set {@code source_model} or {@code similarity_function}; those have the
 * dedicated {@code sourceModel} / {@code metric} fields. Values are Strings because CQL index
 * options are a {@code Map<String, String>}.
 *
 * <p>This is an initial in-code set; the values are expected to be tuned and moved to config.
 */
public final class VectorIndexProfiles {

  private VectorIndexProfiles() {}

  /** Profile name (lower-cased for case-insensitive lookup) to CQL indexing options. */
  private static final Map<String, Map<String, String>> PROFILES =
      Map.of(
          "small-high-recall",
          Map.of(
              VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32",
              VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200"),
          "big-low-latency",
          Map.of(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "16"));

  /**
   * Looks up a profile by name, case-insensitively.
   *
   * @return the profile's CQL options, or empty if {@code name} is null, blank, or unknown
   */
  public static Optional<Map<String, String>> forName(String name) {
    if (name == null || name.isBlank()) {
      return Optional.empty();
    }
    return Optional.ofNullable(PROFILES.get(name.toLowerCase()));
  }

  /** Names of all known profiles, for error messages. */
  public static Set<String> knownNames() {
    return PROFILES.keySet();
  }
}
