package io.stargate.sgv2.jsonapi.service.projection;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.io.IOException;
import java.util.*;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DocumentProjectorForIndexingTest {
  final ObjectMapper objectMapper = new ObjectMapper();

  @Nested
  class AllowFiltering {
    @Test
    public void testRootAllow() {
      assertAllowProjection(
          Arrays.asList("d", "b", "y"), // "y" won't match anything
          """
                        { "_id":123, "a": 5, "b": 6, "c": 7, "d": 8 }
                    """,
          """
                        { "b": 6, "d": 8 }
                    """);
    }

    @Test
    public void testNestedAllow() {
      assertAllowProjection(
          Arrays.asList("a.y", "c", "x"), // "x" won't match anything
          """
                      { "_id":123,
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

    // Unlike with projection, overlapping paths are accepted for Indexing
    @Test
    public void testOverlappingAllow() {
      final String INPUT_DOC =
          """
                          {
                            "_id":123,
                            "a": {
                               "b" : {
                                  "z" : true,
                                  "abc": 123
                                }
                             },
                             "b" : 123
                          }
                      """;
      final String EXP_OUTPUT =
          """
                          {
                            "a": {
                               "b" : {
                                  "z" : true,
                                  "abc": 123
                                }
                             }
                          }
                      """;

      // Try with overlapping paths
      assertAllowProjection(Arrays.asList("a", "a.b"), INPUT_DOC, EXP_OUTPUT);
      assertAllowProjection(Arrays.asList("a.b", "a"), INPUT_DOC, EXP_OUTPUT);
      assertAllowProjection(Arrays.asList("a.b.z", "a.b"), INPUT_DOC, EXP_OUTPUT);
      assertAllowProjection(Arrays.asList("a.b", "a.b.z"), INPUT_DOC, EXP_OUTPUT);
      assertAllowProjection(Arrays.asList("a.b.z", "a"), INPUT_DOC, EXP_OUTPUT);
    }
  }

  @Nested
  class DenyFiltering {
    @Test
    public void testRootDeny() {
      assertDenyProjection(
          Arrays.asList("d", "b", "x"), // "x" won't match anything
          """
                        { "_id":123, "a": 5, "b": 6, "c": 7, "d": 8 }
                    """,
          """
                        { "_id":123, "a": 5, "c": 7 }
                    """);
    }

    @Test
    public void testNestedDeny() {
      assertDenyProjection(
          Arrays.asList("a.y", "c", "z"), // "z" non-matching
          """
                      { "_id":123,
                        "a": { "x":1, "y":2 },
                        "b": { "x":3, "y":4 },
                        "c": { "x":2, "y":3 }
                      }
                  """,
          """
                        { "_id":123,
                          "a": { "x":1 },
                          "b": { "x":3, "y":4 }
                        }
                    """);
    }

    @Test
    public void testOverlappingDeny() {
      final String INPUT_DOC =
          """
                          {
                            "_id":123,
                            "a": {
                               "b" : {
                                  "z" : true,
                                  "abc": 123
                                }
                             },
                             "b" : 123
                          }
                      """;
      final String EXP_OUTPUT =
          """
                          {
                            "_id":123,
                             "b" : 123
                          }
                      """;

      // Try with overlapping paths
      assertDenyProjection(Arrays.asList("a", "a.b"), INPUT_DOC, EXP_OUTPUT);
      assertDenyProjection(Arrays.asList("a.b", "a"), INPUT_DOC, EXP_OUTPUT);
      assertDenyProjection(Arrays.asList("a.b.z", "a"), INPUT_DOC, EXP_OUTPUT);
      assertDenyProjection(Arrays.asList("a", "a.b.z"), INPUT_DOC, EXP_OUTPUT);
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

  @Nested
  class PathInclusion {
    @Test
    public void testPathInclusion() {
      IndexingProjector rootProj = createAllowProjection(Arrays.asList("a", "c"));
      assertIsIncluded(rootProj, "a", "a.x", "a.b.c.d");
      assertIsIncluded(rootProj, "c", "c.a", "c.x.y.z");
      assertIsNotIncluded(rootProj, "b", "d", "b.a", "abc", "cef", "_id");

      IndexingProjector nestedProj = createAllowProjection(Arrays.asList("a.b"));
      assertIsIncluded(nestedProj, "a.b", "a.b.c", "a.b.longer.path.for.sure");
      assertIsNotIncluded(nestedProj, "a", "b", "c", "a.c", "a.x", "a.x.y.z", "_id");

      // Let's also check overlapping (redundant) case; most generic used (specific ignored)
      // same as just "c":
      IndexingProjector overlapProj = createAllowProjection(Arrays.asList("c", "c.x"));
      assertIsIncluded(overlapProj, "c", "c.abc", "c.d.e.f");
      assertIsNotIncluded(overlapProj, "a", "b", "d", "a.c", "a.x.y.z", "_id");
    }

    @Test
    public void testPathExclusion() {
      IndexingProjector rootProj = createDenyProjection(Arrays.asList("a", "c"));
      assertIsNotIncluded(rootProj, "a", "a.x", "a.b.c.d");
      assertIsNotIncluded(rootProj, "c", "c.a", "c.x.y.z");
      assertIsIncluded(rootProj, "b", "d", "b.a", "abc", "cef", "_id");

      IndexingProjector nestedProj = createDenyProjection(Arrays.asList("a.b", "a.noindex"));
      assertIsNotIncluded(
          nestedProj, "a.b", "a.b.c", "a.b.longer.path.for.sure", "a.noindex", "a.noindex.x");
      assertIsIncluded(nestedProj, "a", "b", "_id", "a.c", "a.x", "a.x.y.z");

      // Let's also check overlapping (redundant) case; most generic used (specific ignored)
      // same as just "c":
      IndexingProjector overlapProj = createDenyProjection(Arrays.asList("c", "c.x"));
      assertIsNotIncluded(overlapProj, "c", "c.abc", "c.d.e.f");
      assertIsIncluded(overlapProj, "a", "b", "d", "a.c", "a.x.y.z", "_id");
    }
  }

  private void assertAllowProjection(
      Collection<String> allows, String inputDoc, String expectedDoc) {
    IndexingProjector projector = createAllowProjection(allows);
    try {
      JsonNode input = objectMapper.readTree(inputDoc);
      projector.applyProjection(input);
      assertThat(input).isEqualTo(objectMapper.readTree(expectedDoc));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void assertDenyProjection(
      Collection<String> denies, String inputDoc, String expectedDoc) {
    IndexingProjector projector = createDenyProjection(denies);
    try {
      JsonNode input = objectMapper.readTree(inputDoc);
      projector.applyProjection(input);
      assertThat(input).isEqualTo(objectMapper.readTree(expectedDoc));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private IndexingProjector createAllowProjection(Collection<String> allows) {
    return IndexingProjector.createForIndexing(new LinkedHashSet<>(allows), null);
  }

  private IndexingProjector createDenyProjection(Collection<String> denies) {
    return IndexingProjector.createForIndexing(null, new LinkedHashSet<>(denies));
  }

  private void assertIsIncluded(IndexingProjector proj, String... paths) {
    for (String path : paths) {
      assertThat(proj.isPathIncluded(path))
          .withFailMessage("Path '%s' should be included", path)
          .isTrue();
    }
  }

  private void assertIsNotIncluded(IndexingProjector proj, String... paths) {
    for (String path : paths) {
      assertThat(proj.isPathIncluded(path))
          .withFailMessage("Path '%s' should NOT be included", path)
          .isFalse();
    }
  }
}
