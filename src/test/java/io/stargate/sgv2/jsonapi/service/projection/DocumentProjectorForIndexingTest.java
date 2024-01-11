package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentProjectorForIndexingTest {
  @Inject ObjectMapper objectMapper;

  @Nested
  class AllowFiltering {
    @Test
    public void testSimpleRootAllow() {
      assertAllowProjection(
          Arrays.asList("d", "b"),
          """
                        { "a": 5, "b": 6, "c": 7, "d": 8 }
                    """,
          """
                        { "b": 6, "d": 8 }
                    """);
    }

    @Test
    public void testSimpleNestedAllow() {
      assertAllowProjection(
          Arrays.asList("a.y", "c"),
          """
                      {
                        "a": { "x":1, "y":2 },
                        "b": { "x":3, "y":4 },
                        "c": { "x":2, "y":3 }
                      }
                  """,
          """
                        {
                          "a": { "y":2 },
                          "c": { "x":2, "y":3 }
                        }
                    """);
    }
  }

  @Nested
  class DenyFiltering {
    @Test
    public void testSimpleRootDeny() {
      assertDenyProjection(
          Arrays.asList("d", "b"),
          """
                        { "a": 5, "b": 6, "c": 7, "d": 8 }
                    """,
          """
                        { "a": 5, "c": 7 }
                    """);
    }

    @Test
    public void testSimpleNestedDeny() {
      assertDenyProjection(
          Arrays.asList("a.y", "c"),
          """
                      {
                        "a": { "x":1, "y":2 },
                        "b": { "x":3, "y":4 },
                        "c": { "x":2, "y":3 }
                      }
                  """,
          """
                        {
                          "a": { "x":1 },
                          "b": { "x":3, "y":4 }
                        }
                    """);
    }
  }

  @Nested
  class SpecialCases {
    @Test
    public void testAllowAll() {
      final String DOC =
          """
              { "a": 5, "b": { "enabled": true}, "c": 7, "d": [ { "value": 42} ] }
          """;
      // First with empty Sets:
      assertAllowProjection(Arrays.asList(), DOC, DOC);
      // And then "*" notation
      assertAllowProjection(Arrays.asList("*"), DOC, DOC);
    }

    @Test
    public void testDenyAll() {
      final String DOC =
          """
              { "a": 5, "b": { "enabled": true}, "c": 7, "d": [ { "value": 42} ] }
          """;
      // Only "*" notation available
      assertDenyProjection(Arrays.asList("*"), DOC, "{ }");
    }
  }

  private void assertAllowProjection(List<String> allows, String inputDoc, String expectedDoc) {
    DocumentProjector projector =
        DocumentProjector.createForIndexing(new LinkedHashSet<>(allows), Collections.emptySet());
    try {
      JsonNode input = objectMapper.readTree(inputDoc);
      projector.applyProjection(input);
      assertThat(input).isEqualTo(objectMapper.readTree(expectedDoc));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertDenyProjection(List<String> denies, String inputDoc, String expectedDoc) {
    DocumentProjector projector =
        DocumentProjector.createForIndexing(Collections.emptySet(), new LinkedHashSet<>(denies));
    try {
      JsonNode input = objectMapper.readTree(inputDoc);
      projector.applyProjection(input);
      assertThat(input).isEqualTo(objectMapper.readTree(expectedDoc));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
