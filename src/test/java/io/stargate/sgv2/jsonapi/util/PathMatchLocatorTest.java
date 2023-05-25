package io.stargate.sgv2.jsonapi.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import java.io.IOException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class PathMatchLocatorTest {
  @Inject protected ObjectMapper objectMapper;

  @Nested
  class HappyPathFindIfExists {
    @Test
    public void findRootPropertyPath() {
      ObjectNode doc = objectFromJson("{\"a\" : 1 }");
      PathMatch target = PathMatchLocator.forPath("a").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // But cannot proceed via atomic node
      target = PathMatchLocator.forPath("a.x").findIfExists(doc);
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();

      // Can refer to missing property as well
      target = PathMatchLocator.forPath("unknown").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      PathMatch target = PathMatchLocator.forPath("1").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(7));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);

      // May try to reference past end, no match
      target = PathMatchLocator.forPath("9").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(9);
    }

    @Test
    public void findNestedPropertyPath() {
      ObjectNode doc =
          objectFromJson(
              """
                    {
                      "b" : {
                         "c" : true
                      }
                    }
                    """);

      // First: simple nested property:
      PathMatch target = PathMatchLocator.forPath("b.c").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isEqualTo(fromJson("true"));
      assertThat(target.lastProperty()).isEqualTo("c");

      // But can also refer to its parent
      target = PathMatchLocator.forPath("b").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectFromJson("{\"c\":true}"));
      assertThat(target.lastProperty()).isEqualTo("b");

      // Or to missing property within existing Object:
      target = PathMatchLocator.forPath("b.unknown").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");

      // But with deeper missing path, no more context
      target = PathMatchLocator.forPath("b.unknown.bogus").findIfExists(doc);
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
    }

    @Test
    public void findNestedArrayPath() {
      ObjectNode doc =
          objectFromJson(
              """
                              {
                                "array" : [ 1, 2,
                                   { "a": 3,
                                      "subArray" : [ true, false ]
                                    }
                                 ]
                              }
                              """);

      // First, existing path
      PathMatch target = PathMatchLocator.forPath("array.0").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isEqualTo(doc.numberNode(1));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(0);

      // Then non-existing index, has context (could add)
      target = PathMatchLocator.forPath("array.5").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isNull();

      // and then non-existing property (no properties in Array); no context (not legal to set)
      target = PathMatchLocator.forPath("array.prop").findIfExists(doc);
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();

      // But we can traverse through multiple nesting levels:
      target = PathMatchLocator.forPath("array.2.subArray.1").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.at("/array/2/subArray"));
      assertThat(target.valueNode()).isEqualTo(doc.booleanNode(false));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);
    }
  }

  @Nested
  class FailForFindIfExists {
    @Test
    public void invalidEmptySegment() {
      Exception e =
          catchException(() -> PathMatchLocator.forPath("a..x").findIfExists(objectFromJson("{}")));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": empty segment ('') in path 'a..x'");
    }
  }

  @Nested
  class HappyPathFindOrCreate {
    @Test
    public void findRootPropertyPath() {
      ObjectNode doc = objectFromJson("{\"a\" : 1 }");
      PathMatch target = PathMatchLocator.forPath("a").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // Can refer to missing property as well
      target = PathMatchLocator.forPath("unknown").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      PathMatch target = PathMatchLocator.forPath("1").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(7));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);

      // May try to reference past end, no match
      target = PathMatchLocator.forPath("9").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(9);
    }

    @Test
    public void findNestedPropertyPath() {
      ObjectNode doc =
          objectFromJson(
              """
                              {
                                "b" : {
                                   "c" : true
                                }
                              }
                              """);

      // First: simple nested property:
      PathMatch target = PathMatchLocator.forPath("b.c").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isEqualTo(fromJson("true"));
      assertThat(target.lastProperty()).isEqualTo("c");

      // But can also go for not eisting
      target = PathMatchLocator.forPath("b.x.y").findOrCreate(doc);
      // will now have created path
      assertThat(target.contextNode()).isSameAs(doc.at("/b/x"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("y");
    }

    @Test
    public void findNestedArrayPath() {
      ObjectNode doc =
          objectFromJson(
              """
                              {
                                "array" : [ 1, 2,
                                   { "a": 3,
                                      "subArray" : [ true, false ]
                                    }
                                 ]
                              }
                              """);

      // First, existing path
      PathMatch target = PathMatchLocator.forPath("array.0").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isEqualTo(doc.numberNode(1));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(0);

      // But then something past inner array's end
      target = PathMatchLocator.forPath("array.2.subArray.3").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc.at("/array/2/subArray"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(3);
    }
  }

  @Nested
  class FailForFindOrCreate {
    @Test
    public void invalidPathViaAtomic() {
      Exception e =
          catchException(
              () -> PathMatchLocator.forPath("a.x").findOrCreate(objectFromJson("{\"a\": 3}")));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('x') in path 'a.x'; only OBJECT nodes have properties (got NUMBER)");

      e =
          catchException(
              () ->
                  PathMatchLocator.forPath("a.b.c.d")
                      .findOrCreate(objectFromJson("{\"a\": {\"b\": null}}")));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('c') in path 'a.b.c.d'; only OBJECT nodes have properties (got NULL)");
    }

    @Test
    public void invalidPropViaArray() {
      Exception e =
          catchException(
              () ->
                  PathMatchLocator.forPath("array.prop")
                      .findOrCreate(objectFromJson("{\"array\": [1] }")));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('prop') in path 'array.prop'; only OBJECT nodes have properties (got ARRAY)");

      e =
          catchException(
              () ->
                  PathMatchLocator.forPath("ob.array.0.a2.x")
                      .findOrCreate(objectFromJson("{\"ob\":{\"array\":[{\"a2\":[true]}]}} }")));
      assertThat(e)
          .isNotNull()
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH)
          .hasMessage(
              ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                  + ": cannot create field ('x') in path 'ob.array.0.a2.x'; only OBJECT nodes have properties (got ARRAY)");
    }
  }

  @Nested
  class HappyPathFindValueIn {
    @Test
    public void findRootPropertyPath() {
      ObjectNode doc = objectFromJson("{\"a\" : 1 }");
      JsonNode value = PathMatchLocator.forPath("a").findValueIn(doc);
      assertThat(value).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // But cannot proceed via atomic node
      value = PathMatchLocator.forPath("a.x").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      // Can refer to missing property as well
      value = PathMatchLocator.forPath("unknown").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      JsonNode value = PathMatchLocator.forPath("1").findValueIn(doc);
      assertThat(value).isEqualTo(objectMapper.getNodeFactory().numberNode(7));

      // May try to reference past end, no match
      value = PathMatchLocator.forPath("9").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();
    }

    @Test
    public void findNestedPropertyPath() {
      ObjectNode doc =
          objectFromJson(
              """
                            {
                              "b" : {
                                 "c" : true
                              }
                            }
                            """);

      // First: simple nested property:
      JsonNode value = PathMatchLocator.forPath("b.c").findValueIn(doc);
      assertThat(value).isEqualTo(fromJson("true"));

      // But can also refer to its parent
      value = PathMatchLocator.forPath("b").findValueIn(doc);
      assertThat(value).isEqualTo(objectFromJson("{\"c\":true}"));

      value = PathMatchLocator.forPath("b.unknown").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      value = PathMatchLocator.forPath("b.unknown.bogus").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();
    }

    @Test
    public void findNestedArrayPath() {
      ObjectNode doc =
          objectFromJson(
              """
                                      {
                                        "array" : [ 1, 2,
                                           { "a": 3,
                                              "subArray" : [ true, false ]
                                            }
                                         ]
                                      }
                                      """);

      // First, existing path
      JsonNode value = PathMatchLocator.forPath("array.0").findValueIn(doc);
      assertThat(value).isEqualTo(doc.numberNode(1));

      // Then non-existing index
      value = PathMatchLocator.forPath("array.5").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      // and then non-existing property (no properties in Array)
      value = PathMatchLocator.forPath("array.prop").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      // But we can traverse through multiple nesting levels:
      value = PathMatchLocator.forPath("array.2.subArray.1").findValueIn(doc);
      assertThat(value).isEqualTo(doc.booleanNode(false));
    }
  }

  @Nested
  class Sorting {
    @Test
    public void testOrdering() {
      // Simple alphabetic ordering in general
      verifyOrdering(PathMatchLocator.forPath("abc"), PathMatchLocator.forPath("def"));
      verifyOrdering(PathMatchLocator.forPath("root.x"), PathMatchLocator.forPath("root.y"));
      // Longer one later (if common prefix; parent/child)
      verifyOrdering(PathMatchLocator.forPath("root"), PathMatchLocator.forPath("rootValue"));
      verifyOrdering(PathMatchLocator.forPath("root.abc"), PathMatchLocator.forPath("root.abcdef"));
      verifyOrdering(PathMatchLocator.forPath("root.a"), PathMatchLocator.forPath("root.a.3"));

      // Important! Need to ensure "dot-segment" sorts before continued value; "$" and "+"
      // particular
      // concerns as their ASCII/Unicode value below comma
      verifyOrdering(PathMatchLocator.forPath("root.a"), PathMatchLocator.forPath("root$a"));
      verifyOrdering(PathMatchLocator.forPath("x.y"), PathMatchLocator.forPath("x+y.a"));
    }

    private void verifyOrdering(PathMatchLocator loc1, PathMatchLocator loc2) {
      assertThat(loc1.compareTo(loc2)).isLessThan(0);
      assertThat(loc2.compareTo(loc1)).isGreaterThan(0);
    }

    @Test
    public void testSubPath() {
      // Not sub-path, no dot separator
      verifyNotSubPath(PathMatchLocator.forPath("root"), PathMatchLocator.forPath("rootValue"));
      // Same path is NOT sub-path
      verifyNotSubPath(PathMatchLocator.forPath("root"), PathMatchLocator.forPath("root"));
      // Nor siblings
      verifyNotSubPath(PathMatchLocator.forPath("root.x"), PathMatchLocator.forPath("root.y"));
      // Nor with "weird" characters
      verifyNotSubPath(PathMatchLocator.forPath("root"), PathMatchLocator.forPath("root$x"));

      // But with dot, is
      verifyIsSubPath(PathMatchLocator.forPath("root"), PathMatchLocator.forPath("root.Value"));
      verifyIsSubPath(PathMatchLocator.forPath("root"), PathMatchLocator.forPath("root.x.y.z"));
      verifyIsSubPath(
          PathMatchLocator.forPath("root.branch"), PathMatchLocator.forPath("root.branch.4"));
    }

    private void verifyIsSubPath(PathMatchLocator parent, PathMatchLocator child) {
      // One way it is true; the other not
      assertThat(child.isSubPathOf(parent)).isTrue();
      assertThat(parent.isSubPathOf(child)).isFalse();
    }

    private void verifyNotSubPath(PathMatchLocator parent, PathMatchLocator child) {
      // Neither is true
      assertThat(child.isSubPathOf(parent)).isFalse();
      assertThat(parent.isSubPathOf(child)).isFalse();
    }
  }

  protected ObjectNode objectFromJson(String json) {
    return (ObjectNode) fromJson(json);
  }

  protected JsonNode fromJson(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
