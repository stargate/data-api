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
 * <p>Initial in-code set; values to be tuned and moved to config (<a
 * href="https://github.com/stargate/data-api/issues/2508">#2508</a>).
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

  /**
   * Reverse lookup: the profile whose expanded options exactly match {@code options}, used on
   * read-back to label an index that was created from a known profile. Exact match only, so an
   * index whose options differ from, or are a superset of, a profile reports its raw options
   * instead. The stored options are not persisted, so this is a best-effort reconstruction (<a
   * href="https://github.com/stargate/data-api/issues/2508">#2508</a>).
   *
   * @return the matching profile name, or empty if {@code options} is null/empty or matches none
   */
  public static Optional<String> detect(Map<String, String> options) {
    if (options == null || options.isEmpty()) {
      return Optional.empty();
    }
    return PROFILES.entrySet().stream()
        .filter(entry -> entry.getValue().equals(options))
        .map(Map.Entry::getKey)
        .findFirst();
  }
}
