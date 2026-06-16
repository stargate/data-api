package io.stargate.sgv2.jsonapi.service.schema.tables;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.sgv2.jsonapi.config.constants.VectorConstants;
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
}
