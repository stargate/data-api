package io.stargate.sgv2.jsonapi.service.operation.model.command.clause.update;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTarget;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTargetLocator;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class UpdateTargetLocatorTest extends UpdateOperationTestBase {
  @Nested
  class HappyPathFindIfExists {
    @Test
    public void findRootPropertyPath() {
      ObjectNode doc = objectFromJson("{\"a\" : 1 }");
      UpdateTarget target = UpdateTargetLocator.forPath("a").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // But cannot proceed via atomic node
      target = UpdateTargetLocator.forPath("a.x").findIfExists(doc);
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();

      // Can refer to missing property as well
      target = UpdateTargetLocator.forPath("unknown").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      UpdateTarget target = UpdateTargetLocator.forPath("1").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(7));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);

      // May try to reference past end, no match
      target = UpdateTargetLocator.forPath("9").findIfExists(doc);
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
      UpdateTarget target = UpdateTargetLocator.forPath("b.c").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isEqualTo(fromJson("true"));
      assertThat(target.lastProperty()).isEqualTo("c");

      // But can also refer to its parent
      target = UpdateTargetLocator.forPath("b").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectFromJson("{\"c\":true}"));
      assertThat(target.lastProperty()).isEqualTo("b");

      // Or to missing property within existing Object:
      target = UpdateTargetLocator.forPath("b.unknown").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");

      // But with deeper missing path, no more context
      target = UpdateTargetLocator.forPath("b.unknown.bogus").findIfExists(doc);
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
      UpdateTarget target = UpdateTargetLocator.forPath("array.0").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isEqualTo(doc.numberNode(1));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(0);

      // Then non-existing index, has context (could add)
      target = UpdateTargetLocator.forPath("array.5").findIfExists(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isNull();

      // and then non-existing property (no properties in Array); no context (not legal to set)
      target = UpdateTargetLocator.forPath("array.prop").findIfExists(doc);
      assertThat(target.contextNode()).isNull();
      assertThat(target.valueNode()).isNull();

      // But we can traverse through multiple nesting levels:
      target = UpdateTargetLocator.forPath("array.2.subArray.1").findIfExists(doc);
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
          catchException(
              () -> UpdateTargetLocator.forPath("a..x").findIfExists(objectFromJson("{}")));
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
      UpdateTarget target = UpdateTargetLocator.forPath("a").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // Can refer to missing property as well
      target = UpdateTargetLocator.forPath("unknown").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isNull();
      assertThat(target.lastProperty()).isEqualTo("unknown");
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      UpdateTarget target = UpdateTargetLocator.forPath("1").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc);
      assertThat(target.valueNode()).isEqualTo(objectMapper.getNodeFactory().numberNode(7));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(1);

      // May try to reference past end, no match
      target = UpdateTargetLocator.forPath("9").findOrCreate(doc);
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
      UpdateTarget target = UpdateTargetLocator.forPath("b.c").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("b"));
      assertThat(target.valueNode()).isEqualTo(fromJson("true"));
      assertThat(target.lastProperty()).isEqualTo("c");

      // But can also go for not eisting
      target = UpdateTargetLocator.forPath("b.x.y").findOrCreate(doc);
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
      UpdateTarget target = UpdateTargetLocator.forPath("array.0").findOrCreate(doc);
      assertThat(target.contextNode()).isSameAs(doc.get("array"));
      assertThat(target.valueNode()).isEqualTo(doc.numberNode(1));
      assertThat(target.lastProperty()).isNull();
      assertThat(target.lastIndex()).isEqualTo(0);

      // But then something past inner array's end
      target = UpdateTargetLocator.forPath("array.2.subArray.3").findOrCreate(doc);
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
              () -> UpdateTargetLocator.forPath("a.x").findOrCreate(objectFromJson("{\"a\": 3}")));
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
                  UpdateTargetLocator.forPath("a.b.c.d")
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
                  UpdateTargetLocator.forPath("array.prop")
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
                  UpdateTargetLocator.forPath("ob.array.0.a2.x")
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
      JsonNode value = UpdateTargetLocator.forPath("a").findValueIn(doc);
      assertThat(value).isEqualTo(objectMapper.getNodeFactory().numberNode(1));

      // But cannot proceed via atomic node
      value = UpdateTargetLocator.forPath("a.x").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      // Can refer to missing property as well
      value = UpdateTargetLocator.forPath("unknown").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();
    }

    @Test
    public void findRootIndexPath() {
      // Although main-level is always an Object, locator is not limited
      // and can refer to array indexes too
      JsonNode doc = fromJson("[ 3, 7 ]");
      JsonNode value = UpdateTargetLocator.forPath("1").findValueIn(doc);
      assertThat(value).isEqualTo(objectMapper.getNodeFactory().numberNode(7));

      // May try to reference past end, no match
      value = UpdateTargetLocator.forPath("9").findValueIn(doc);
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
      JsonNode value = UpdateTargetLocator.forPath("b.c").findValueIn(doc);
      assertThat(value).isEqualTo(fromJson("true"));

      // But can also refer to its parent
      value = UpdateTargetLocator.forPath("b").findValueIn(doc);
      assertThat(value).isEqualTo(objectFromJson("{\"c\":true}"));

      value = UpdateTargetLocator.forPath("b.unknown").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      value = UpdateTargetLocator.forPath("b.unknown.bogus").findValueIn(doc);
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
      JsonNode value = UpdateTargetLocator.forPath("array.0").findValueIn(doc);
      assertThat(value).isEqualTo(doc.numberNode(1));

      // Then non-existing index
      value = UpdateTargetLocator.forPath("array.5").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      // and then non-existing property (no properties in Array)
      value = UpdateTargetLocator.forPath("array.prop").findValueIn(doc);
      assertThat(value.isMissingNode()).isTrue();

      // But we can traverse through multiple nesting levels:
      value = UpdateTargetLocator.forPath("array.2.subArray.1").findValueIn(doc);
      assertThat(value).isEqualTo(doc.booleanNode(false));
    }
  }
}
