package io.stargate.sgv2.jsonapi.service.schema.tables;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class VectorIndexProfilesTest {

  @Nested
  class ForName {
    @Test
    @DisplayName("forName returns options for a known profile")
    void knownProfile() {
      assertThat(VectorIndexProfiles.forName("small-high-recall"))
          .isPresent()
          .get()
          .satisfies(opts -> assertThat(opts).isNotEmpty());
    }

    @Test
    @DisplayName("forName is case-insensitive")
    void caseInsensitive() {
      assertThat(VectorIndexProfiles.forName("SMALL-HIGH-RECALL"))
          .isEqualTo(VectorIndexProfiles.forName("small-high-recall"));
    }

    @Test
    @DisplayName("forName returns empty for an unknown profile")
    void unknownProfile() {
      assertThat(VectorIndexProfiles.forName("does-not-exist")).isEmpty();
    }

    @Test
    @DisplayName("forName returns empty for null or blank")
    void nullOrBlank() {
      assertThat(VectorIndexProfiles.forName(null)).isEmpty();
      assertThat(VectorIndexProfiles.forName("  ")).isEmpty();
    }
  }

  @Nested
  class KnownNames {
    @Test
    @DisplayName("knownNames lists the available profiles")
    void listsProfiles() {
      assertThat(VectorIndexProfiles.knownNames()).contains("small-high-recall", "big-low-latency");
    }
  }

  @Nested
  class ProfileContents {
    @Test
    @DisplayName("profiles never set the reserved (dedicated-field) options")
    void noReservedOptions() {
      for (var name : VectorIndexProfiles.knownNames()) {
        var options = VectorIndexProfiles.forName(name).orElseThrow();
        assertThat(options.keySet())
            .doesNotContainAnyElementsOf(VectorConstants.CQLAnnIndex.RESERVED_OPTIONS);
      }
    }
  }
}
