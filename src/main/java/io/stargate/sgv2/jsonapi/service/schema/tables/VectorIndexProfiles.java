package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Named vector-index profiles, each mapping a profile name to Cassandra SAI indexing options. An
 * alternative to passing raw options through {@code vectorIndexing}.
 *
 * <p>Profiles never set {@code source_model} or {@code similarity_function}; those have the
 * dedicated {@code sourceModel} / {@code metric} fields. Values are Strings because CQL index
 * options are a {@code Map<String, String>}.
 *
 * <p>Initial in-code set; values to be tuned and moved to config.
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
   * Case-insensitive profile lookup.
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
