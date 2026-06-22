package io.stargate.sgv2.jsonapi.service.schema.tables;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class VectorIndexProfilesTest {

  @Nested
  class ForName {
    @Test
    void knownProfile() {
      assertThat(VectorIndexProfiles.forName("small-high-recall"))
          .isPresent()
          .get()
          .satisfies(opts -> assertThat(opts).isNotEmpty());
    }

    @Test
    void caseInsensitive() {
      assertThat(VectorIndexProfiles.forName("SMALL-HIGH-RECALL"))
          .isEqualTo(VectorIndexProfiles.forName("small-high-recall"));
    }

    @Test
    void unknownProfile() {
      assertThat(VectorIndexProfiles.forName("does-not-exist")).isEmpty();
    }

    @Test
    void nullOrBlank() {
      assertThat(VectorIndexProfiles.forName(null)).isEmpty();
      assertThat(VectorIndexProfiles.forName("  ")).isEmpty();
    }
  }

  @Nested
  class KnownNames {
    @Test
    void listsProfiles() {
      assertThat(VectorIndexProfiles.knownNames()).contains("small-high-recall", "big-low-latency");
    }
  }

  @Nested
  class ProfileContents {
    @Test
    void noReservedOptions() {
      for (var name : VectorIndexProfiles.knownNames()) {
        var options = VectorIndexProfiles.forName(name).orElseThrow();
        assertThat(options.keySet())
            .doesNotContainAnyElementsOf(VectorConstants.CQLAnnIndex.RESERVED_OPTIONS);
      }
    }
  }

  @Nested
  class Detect {
    @Test
    void exactMatchReturnsProfile() {
      var smallHighRecall = VectorIndexProfiles.forName("small-high-recall").orElseThrow();
      assertThat(VectorIndexProfiles.detect(smallHighRecall)).contains("small-high-recall");
    }

    @Test
    void noMatchWhenOptionsDiffer() {
      assertThat(
              VectorIndexProfiles.detect(
                  Map.of(VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "20")))
          .isEmpty();
    }

    @Test
    void noMatchWhenSupersetOfAProfile() {
      // a superset of small-high-recall is not an exact match
      assertThat(
              VectorIndexProfiles.detect(
                  Map.of(
                      VectorConstants.CQLAnnIndex.MAXIMUM_NODE_CONNECTIONS, "32",
                      VectorConstants.CQLAnnIndex.CONSTRUCTION_BEAM_WIDTH, "200",
                      VectorConstants.CQLAnnIndex.ALPHA, "1.2")))
          .isEmpty();
    }

    @Test
    void emptyOrNull() {
      assertThat(VectorIndexProfiles.detect(Map.of())).isEmpty();
      assertThat(VectorIndexProfiles.detect(null)).isEmpty();
    }
  }
}
