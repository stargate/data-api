package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class VectorIndexProfileDefinitionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nested
  class FromJson {

    @Test
    void nullOrBlankIsEmpty() {
      assertThat(VectorIndexProfileDefinition.fromJson(null, MAPPER)).isEmpty();
      assertThat(VectorIndexProfileDefinition.fromJson("   ", MAPPER)).isEmpty();
    }

    @Test
    void parsesNameAndOptions() {
      var json =
          "{\"my_idx\":{\"profile\":\"small-high-recall\","
              + "\"options\":{\"maximum_node_connections\":\"32\"}}}";

      var defs = VectorIndexProfileDefinition.fromJson(json, MAPPER);

      assertThat(defs).containsOnlyKeys("my_idx");
      assertThat(defs.get("my_idx").profile()).isEqualTo("small-high-recall");
      assertThat(defs.get("my_idx").options()).containsEntry("maximum_node_connections", "32");
    }

    @Test
    void malformedJsonIsEmpty() {
      // advisory metadata: bad JSON must not fail the read
      assertThat(VectorIndexProfileDefinition.fromJson("not json", MAPPER)).isEmpty();
    }

    @Test
    void roundTripThroughObjectMapper() throws Exception {
      Map<String, VectorIndexProfileDefinition> original = new HashMap<>();
      original.put(
          "idx",
          new VectorIndexProfileDefinition(
              "big-low-latency", Map.of("maximum_node_connections", "16")));

      var json = MAPPER.writeValueAsString(original);

      assertThat(VectorIndexProfileDefinition.fromJson(json, MAPPER)).isEqualTo(original);
    }
  }

  @Nested
  class PutOrRemove {

    @Test
    void putNewReturnsChanged() {
      var profiles = new HashMap<String, VectorIndexProfileDefinition>();
      var def = new VectorIndexProfileDefinition("p", Map.of("a", "1"));

      assertThat(VectorIndexProfileDefinition.putOrRemove(profiles, "idx", def)).isTrue();
      assertThat(profiles).containsEntry("idx", def);
    }

    @Test
    void putIdenticalReturnsUnchanged() {
      var profiles = new HashMap<String, VectorIndexProfileDefinition>();
      profiles.put("idx", new VectorIndexProfileDefinition("p", Map.of("a", "1")));

      assertThat(
              VectorIndexProfileDefinition.putOrRemove(
                  profiles, "idx", new VectorIndexProfileDefinition("p", Map.of("a", "1"))))
          .isFalse();
    }

    @Test
    void removeExistingReturnsChanged() {
      var profiles = new HashMap<String, VectorIndexProfileDefinition>();
      profiles.put("idx", new VectorIndexProfileDefinition("p", Map.of()));

      assertThat(VectorIndexProfileDefinition.putOrRemove(profiles, "idx", null)).isTrue();
      assertThat(profiles).doesNotContainKey("idx");
    }

    @Test
    void removeMissingReturnsUnchanged() {
      var profiles = new HashMap<String, VectorIndexProfileDefinition>();

      assertThat(VectorIndexProfileDefinition.putOrRemove(profiles, "idx", null)).isFalse();
    }
  }
}
