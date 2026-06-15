package io.stargate.sgv2.jsonapi.service.schema.tables;

import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Registry of named vector-index "profiles": each maps a user-facing profile name to a set of
 * Cassandra SAI indexing options (the {@code WITH OPTIONS = {...}} entries). Selecting a profile is
 * an alternative to passing raw options through the {@code indexingOptions} field on the
 * createVectorIndex command.
 *
 * <p>Profiles only set the tuning options; they never set {@code source_model} or {@code
 * similarity_function}, which have dedicated API fields ({@code sourceModel} / {@code metric}).
 * Values are stored as Strings because CQL index options are a {@code Map<String, String>}.
 *
 * <p>NOTE: the concrete mappings below are an initial in-code starter set; the values are expected
 * to be tuned and eventually externalised to configuration.
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
   * @param name the profile name from the user request, may be null or blank
   * @return the CQL options for the profile, or empty if the name is null, blank, or not known
   */
  public static Optional<Map<String, String>> forName(String name) {
    if (name == null || name.isBlank()) {
      return Optional.empty();
    }
    return Optional.ofNullable(PROFILES.get(name.toLowerCase()));
  }

  /** Names of all known profiles, for use in error messages. */
  public static Set<String> knownNames() {
    return PROFILES.keySet();
  }
}
